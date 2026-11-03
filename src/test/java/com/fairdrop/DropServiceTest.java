package com.fairdrop;

import com.fairdrop.cache.DropCache;
import com.fairdrop.exception.DropNotFoundException;
import com.fairdrop.exception.QueueEntryNotFoundException;
import com.fairdrop.model.Drop;
import com.fairdrop.model.DropStatus;
import com.fairdrop.observability.FairDropMetrics;
import com.fairdrop.repository.FairDropRepository;
import com.fairdrop.repository.InMemoryFairDropRepository;
import com.fairdrop.service.DropService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link DropService} backed by the real {@link InMemoryFairDropRepository},
 * covering queue fairness under concurrency, idempotent joins, and not-found error cases.
 */
class DropServiceTest {

    private static DropService newDropService(FairDropRepository repository) {
        return new DropService(
                repository,
                new NoOpDropCache(),
                event -> {
                },
                new FairDropMetrics(new SimpleMeterRegistry()));
    }

    /**
     * Many users joining at the same moment must each get a distinct sequence number.
     */
    @Test
    void concurrentUsersShouldReceiveUniqueSequenceNumbers() throws Exception {
        FairDropRepository repository = new InMemoryFairDropRepository();
        DropService dropService = newDropService(repository);
        Drop drop = dropService.createDrop("Football", 100);
        String dropId = drop.getId();
        int numberOfUsers = 1000;

        ExecutorService executor = Executors.newFixedThreadPool(50);
        // Holds every task at the start line so the joins run as concurrently as possible.
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Long>> futures = new ArrayList<>();

        for (int i = 0; i < numberOfUsers; i++) {
            int userNumber = i;

            Future<Long> future = executor.submit(() -> {
                startLatch.await();
                return dropService.joinDrop(dropId, "USER-" + userNumber).getSequenceNumber();
            });

            futures.add(future);
        }

        startLatch.countDown();

        // Any duplicate sequence number would collapse in the set and reduce its size.
        Set<Long> sequenceNumbers = new HashSet<>();
        for (Future<Long> future : futures) {
            sequenceNumbers.add(future.get());
        }

        executor.shutdown();

        assertEquals(numberOfUsers, sequenceNumbers.size());
    }

    /**
     * Joining a drop id that was never created must be rejected.
     */
    @Test
    void joiningUnknownDropShouldThrowDropNotFoundException() {
        FairDropRepository repository = new InMemoryFairDropRepository();
        DropService dropService = newDropService(repository);

        assertThrows(DropNotFoundException.class, () -> dropService.joinDrop("UNKNOWN-DROP", "USER-A"));
    }

    /**
     * Looking up a user who never joined an existing drop must be rejected.
     */
    @Test
    void requestingUserWhoNeverJoinedShouldThrowQueueEntryNotFoundException() {
        FairDropRepository repository = new InMemoryFairDropRepository();
        DropService dropService = newDropService(repository);
        Drop drop = dropService.createDrop("Football", 100);

        assertThrows(QueueEntryNotFoundException.class, () -> dropService.getQueueEntry(drop.getId(), "USER-A"));
    }

    /**
     * Joining is idempotent: a repeat join keeps the user's original queue position.
     */
    @Test
    void sameUserJoiningTwiceShouldReceiveSameSequenceNumber() {
        FairDropRepository repository = new InMemoryFairDropRepository();
        DropService dropService = newDropService(repository);
        Drop drop = dropService.createDrop("Football", 100);
        String dropId = drop.getId();

        long firstSequence = dropService.joinDrop(dropId, "USER-A").getSequenceNumber();
        long secondSequence = dropService.joinDrop(dropId, "USER-A").getSequenceNumber();

        assertEquals(firstSequence, secondSequence);
    }

    @Test
    void shouldReturnExistingDrop() {
        FairDropRepository repository = new InMemoryFairDropRepository();
        DropService dropService = newDropService(repository);

        Drop drop = dropService.createDrop("Football", 100);

        Drop result = dropService.getDrop(drop.getId());

        assertEquals(drop.getId(), result.getId());
        assertEquals("Football", result.getName());
        assertEquals(100, result.getTotalInventory());
        assertEquals(DropStatus.OPEN, result.getStatus());
    }

    private static final class NoOpDropCache implements DropCache {

        @Override
        public Optional<Drop> find(String dropId) {
            return Optional.empty();
        }

        @Override
        public void put(Drop drop) {
        }

        @Override
        public void evict(String dropId) {
        }
    }
}
