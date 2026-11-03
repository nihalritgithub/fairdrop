import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bounded, closed-loop load generator for a running FairDrop instance. JDK only, no dependencies.
 *
 * <pre>
 * java load-test/FairDropLoadTest.java --scenario join --requests 2000 --concurrency 50
 *
 * --base-url     http://localhost:8080
 * --scenario     join | join-rate-limit | read | allocate     (default join)
 * --requests     requests in the measured phase               (default 500,  max 20000)
 * --concurrency  requests in flight at once                   (default 20,   max 200)
 * --inventory    inventory of the drop the run creates        (default 100)
 * --users        users pre-joined by the allocate scenario    (default 200,  max 20000)
 * --timeout      per-request timeout in seconds               (default 10)
 * </pre>
 *
 * Every request carries an {@code X-Correlation-Id} of the form {@code loadtest-<run>-<n>}, so
 * slow or failed requests can be found in the application logs. While a run is in progress, watch
 * {@code /actuator/prometheus} for {@code hikaricp_connections_pending},
 * {@code fairdrop_allocation_lock_wait_seconds}, {@code fairdrop_join_sequence_lock_wait_seconds},
 * {@code fairdrop_ratelimit_check_seconds}, {@code fairdrop_cache_requests_total},
 * {@code fairdrop_join_rate_limited_total} and {@code fairdrop_outbox_pending}.
 */
public final class FairDropLoadTest {

    private static final int MAX_REQUESTS = 20_000;
    private static final int MAX_CONCURRENCY = 200;
    private static final Pattern ID = Pattern.compile("\"id\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern STATUS = Pattern.compile("\"status\"\\s*:\\s*\"([A-Z_]+)\"");
    private static final Pattern USER = Pattern.compile("\"userId\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern ALLOCATED_COUNT = Pattern.compile("\"allocatedCount\"\\s*:\\s*(\\d+)");

    private final Options options;
    private final HttpClient client;
    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private final AtomicInteger requestCounter = new AtomicInteger();

    private FairDropLoadTest(Options options) {
        this.options = options;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        System.out.printf("FairDrop load test %s%n", options);
        new FairDropLoadTest(options).run();
    }

    private void run() throws Exception {
        String dropId = createDrop();
        System.out.printf("run=%s dropId=%s%n", runId, dropId);

        switch (options.scenario()) {
            case "join" -> report("join", execute(options.requests(), i -> join(dropId, "lt-" + runId + "-" + i)));
            case "join-rate-limit" -> report("join (single user)",
                    execute(options.requests(), i -> join(dropId, "lt-" + runId + "-hot-user")));
            case "read" -> {
                send(get("/drops/" + dropId));
                report("read", execute(options.requests(), i -> get("/drops/" + dropId)));
            }
            case "allocate" -> runAllocation(dropId);
            default -> throw new IllegalArgumentException("Unknown scenario: " + options.scenario());
        }
    }

    private void runAllocation(String dropId) throws Exception {
        Results joins = execute(options.users(), i -> join(dropId, "lt-" + runId + "-" + i));
        report("pre-join", joins);

        Results allocations = execute(options.requests(), i -> post("/drops/" + dropId + "/allocate-next"));
        report("allocate-next", allocations);

        Map<String, Integer> outcomes = new TreeMap<>();
        Map<String, Integer> winners = new HashMap<>();
        for (Sample sample : allocations.samples()) {
            outcomes.merge(match(STATUS, sample.body()).orElse("HTTP_" + sample.status()), 1, Integer::sum);
            match(USER, sample.body()).ifPresent(user -> winners.merge(user, 1, Integer::sum));
        }

        int allocated = outcomes.getOrDefault("ALLOCATED", 0);
        long joined = joins.samples().stream().filter(sample -> sample.status() == 200).count();
        long expected = Math.min(options.inventory(), joined);
        long duplicateWinners = winners.values().stream().filter(count -> count > 1).count();
        String drop = send(get("/drops/" + dropId)).body();
        int allocatedCount = Integer.parseInt(match(ALLOCATED_COUNT, drop).orElse("-1"));

        System.out.printf("outcomes=%s%n", outcomes);
        System.out.printf("consistency: allocated=%d expected<=%d dropAllocatedCount=%d duplicateWinners=%d -> %s%n",
                allocated, expected, allocatedCount, duplicateWinners,
                allocated <= options.inventory() && allocated == allocatedCount && duplicateWinners == 0
                        ? "OK" : "VIOLATION");
    }

    private Results execute(int count, IntFunction<HttpRequest> requestFactory) throws InterruptedException {
        ConcurrentLinkedQueue<Sample> samples = new ConcurrentLinkedQueue<>();
        Semaphore inFlight = new Semaphore(options.concurrency());
        long started = System.nanoTime();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < count; i++) {
                inFlight.acquire();
                int index = i;
                executor.submit(() -> {
                    try {
                        samples.add(timed(requestFactory.apply(index)));
                    } finally {
                        inFlight.release();
                    }
                });
            }
        }
        return new Results(new ArrayList<>(samples), System.nanoTime() - started);
    }

    private Sample timed(HttpRequest request) {
        long start = System.nanoTime();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Sample(response.statusCode(), System.nanoTime() - start, response.body());
        } catch (Exception ex) {
            return new Sample(-1, System.nanoTime() - start, ex.getClass().getSimpleName());
        }
    }

    private String createDrop() throws Exception {
        HttpRequest request = builder("/drops")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"name\":\"load-test-" + runId + "\",\"inventory\":" + options.inventory() + "}"))
                .build();
        HttpResponse<String> response = send(request);
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Could not create drop: HTTP " + response.statusCode() + " " + response.body());
        }
        return match(ID, response.body()).orElseThrow();
    }

    private HttpRequest join(String dropId, String userId) {
        return builder("/drops/" + dropId + "/join?userId=" + userId)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    private HttpRequest post(String path) {
        return builder(path).POST(HttpRequest.BodyPublishers.noBody()).build();
    }

    private HttpRequest get(String path) {
        return builder(path).GET().build();
    }

    private HttpRequest.Builder builder(String path) {
        return HttpRequest.newBuilder(options.baseUrl().resolve(path))
                .timeout(options.timeout())
                .header("X-Correlation-Id", "loadtest-" + runId + "-" + requestCounter.incrementAndGet());
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void report(String phase, Results results) {
        List<Long> latencies = new ArrayList<>(results.samples().stream().map(Sample::nanos).toList());
        Collections.sort(latencies);
        Map<Integer, Integer> statuses = new TreeMap<>();
        results.samples().forEach(sample -> statuses.merge(sample.status(), 1, Integer::sum));

        double seconds = results.wallNanos() / 1e9;
        System.out.printf("%n[%s] requests=%d wall=%.2fs throughput=%.1f req/s%n",
                phase, latencies.size(), seconds, latencies.size() / Math.max(seconds, 1e-9));
        System.out.printf("  status codes (-1 = client error/timeout): %s%n", statuses);
        System.out.printf("  latency ms: p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f%n",
                percentile(latencies, 50), percentile(latencies, 90), percentile(latencies, 95),
                percentile(latencies, 99), percentile(latencies, 100));
    }

    private static double percentile(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1))) / 1e6;
    }

    private static Optional<String> match(Pattern pattern, String body) {
        Matcher matcher = pattern.matcher(body == null ? "" : body);
        return matcher.find() ? Optional.of(matcher.group(1)) : Optional.empty();
    }

    private record Sample(int status, long nanos, String body) {
    }

    private record Results(List<Sample> samples, long wallNanos) {
    }

    private record Options(
            URI baseUrl,
            String scenario,
            int requests,
            int concurrency,
            int inventory,
            int users,
            Duration timeout) {

        static Options parse(String[] args) {
            Map<String, String> values = new HashMap<>();
            for (int i = 0; i + 1 < args.length; i += 2) {
                if (!args[i].startsWith("--")) {
                    throw new IllegalArgumentException("Expected --name value pairs, got: " + args[i]);
                }
                values.put(args[i].substring(2), args[i + 1]);
            }
            if (args.length % 2 != 0) {
                throw new IllegalArgumentException("Missing value for " + args[args.length - 1]);
            }

            return new Options(
                    URI.create(values.getOrDefault("base-url", "http://localhost:8080")),
                    values.getOrDefault("scenario", "join"),
                    bounded(values, "requests", 500, MAX_REQUESTS),
                    bounded(values, "concurrency", 20, MAX_CONCURRENCY),
                    bounded(values, "inventory", 100, 1_000_000),
                    bounded(values, "users", 200, MAX_REQUESTS),
                    Duration.ofSeconds(bounded(values, "timeout", 10, 120)));
        }

        private static int bounded(Map<String, String> values, String name, int defaultValue, int max) {
            int value = Integer.parseInt(values.getOrDefault(name, String.valueOf(defaultValue)));
            if (value < 1 || value > max) {
                throw new IllegalArgumentException("--" + name + " must be between 1 and " + max);
            }
            return value;
        }
    }
}
