package com.fairdrop.controller;

import com.fairdrop.dto.AllocationResponse;
import com.fairdrop.dto.CreateDropRequest;
import com.fairdrop.dto.JoinDropResponse;
import com.fairdrop.exception.RateLimitExceededException;
import com.fairdrop.model.AllocationResult;
import com.fairdrop.model.AllocationStatus;
import com.fairdrop.model.Drop;
import com.fairdrop.model.QueueEntry;
import com.fairdrop.ratelimit.JoinRateLimiter;
import com.fairdrop.service.DropService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST entry point for FairDrop under {@code /drops}. Exposes endpoints to create a drop,
 * join its queue, and look up a user's queue position, delegating all logic to {@link DropService}.
 *
 * <p>Identifiers are bounded to the database column size so oversized input is rejected with 400
 * before it reaches Redis keys or SQL.
 */
@RestController
@RequestMapping("/drops")
@Tag(name = "Drops", description = "Create drops, join FIFO queues, and allocate inventory fairly")
public class DropController {

    private static final int MAX_ID_LENGTH = 255;

    private final DropService dropService;
    private final JoinRateLimiter joinRateLimiter;

    public DropController(DropService dropService, JoinRateLimiter joinRateLimiter) {
        this.dropService = dropService;
        this.joinRateLimiter = joinRateLimiter;
    }

    /**
     * Creates a new drop from a validated request body.
     *
     * @param request drop name and available inventory
     * @return the newly created drop
     */
    @Operation(summary = "Create a drop")
    @PostMapping
    public Drop createDrop(@Valid @RequestBody CreateDropRequest request) {
        return dropService.createDrop(request.getName(), request.getInventory());
    }

    /**
     * Adds a user to a drop's queue. Joining again returns the user's existing position.
     *
     * @param dropId identifier of the drop to join
     * @param userId identifier of the user joining
     * @return the user's assigned sequence number in the queue
     */
    @Operation(
            summary = "Join a drop queue",
            description = "Idempotent per user and drop; protected by Redis rate limiting")
    @PostMapping("/{dropId}/join")
    public JoinDropResponse joinDrop(
            @PathVariable @Size(max = MAX_ID_LENGTH) String dropId,
            @RequestParam @NotBlank @Size(max = MAX_ID_LENGTH) String userId) {

        if (!joinRateLimiter.allow(dropId, userId)) {
            throw new RateLimitExceededException();
        }

        QueueEntry entry = dropService.joinDrop(dropId, userId);
        return new JoinDropResponse(entry.getUserId(), entry.getSequenceNumber());
    }

    /**
     * Returns a drop, served from the Redis cache when possible.
     *
     * @param dropId identifier of the drop
     * @return the drop
     */
    @Operation(
            summary = "Get a drop",
            description = "Uses cache-aside Redis reads with PostgreSQL fallback")
    @GetMapping("/{dropId}")
    public Drop getDrop(@PathVariable @Size(max = MAX_ID_LENGTH) String dropId) {
        return dropService.getDrop(dropId);
    }

    /**
     * Looks up the queue position of a user who has already joined a drop.
     *
     * @param dropId identifier of the drop
     * @param userId identifier of the user
     * @return the user's sequence number in the queue
     */
    @Operation(summary = "Get a user queue position")
    @GetMapping("/{dropId}/queue/{userId}")
    public JoinDropResponse getQueueEntry(
            @PathVariable @Size(max = MAX_ID_LENGTH) String dropId,
            @PathVariable @Size(max = MAX_ID_LENGTH) String userId) {

        QueueEntry entry = dropService.getQueueEntry(dropId, userId);
        return new JoinDropResponse(entry.getUserId(), entry.getSequenceNumber());
    }

    /**
     * Allocates one unit of inventory to the earliest queued user who has not been allocated yet.
     *
     * @param dropId identifier of the drop
     * @return the outcome, with the winning user when inventory was allocated
     */
    @Operation(
            summary = "Allocate the next queued user",
            description = "Server selects the earliest eligible user; PostgreSQL locking prevents overselling")
    @PostMapping("/{dropId}/allocate-next")
    public ResponseEntity<AllocationResponse> allocateNext(@PathVariable @Size(max = MAX_ID_LENGTH) String dropId) {

        AllocationResult result = dropService.allocateNext(dropId);

        if (result.getStatus() == AllocationStatus.ALLOCATED) {
            QueueEntry queueEntry = result.getQueueEntry().orElseThrow();

            return ResponseEntity.ok(new AllocationResponse(
                    AllocationStatus.ALLOCATED,
                    queueEntry.getUserId(),
                    queueEntry.getSequenceNumber()));
        }

        return ResponseEntity.ok(new AllocationResponse(result.getStatus(), null, null));
    }
}
