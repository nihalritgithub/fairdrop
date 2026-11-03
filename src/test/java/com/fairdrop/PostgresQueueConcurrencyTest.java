package com.fairdrop;

import com.fairdrop.model.AllocationResult;
import com.fairdrop.model.AllocationStatus;
import com.fairdrop.model.Drop;
import com.fairdrop.service.DropService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class PostgresQueueConcurrencyTest {

    @Autowired
    private DropService dropService;

    @Test
    void concurrentUsersShouldReceiveUniqueSequencesFromPostgres()
            throws Exception {

        Drop drop = dropService.createDrop("Concurrency-Test", 100);

        int numberOfUsers = 100;

        ExecutorService executor =
                Executors.newFixedThreadPool(20);

        CountDownLatch startLatch =
                new CountDownLatch(1);

        List<Future<Long>> futures =
                new ArrayList<>();

        for (int i = 0; i < numberOfUsers; i++) {

            int userNumber = i;

            futures.add(
                    executor.submit(() -> {
                        startLatch.await();

                        return dropService
                                .joinDrop(
                                        drop.getId(),
                                        "USER-" + userNumber
                                )
                                .getSequenceNumber();
                    })
            );
        }

        startLatch.countDown();

        Set<Long> sequenceNumbers = new HashSet<>();

        for (Future<Long> future : futures) {
            sequenceNumbers.add(future.get());
        }

        executor.shutdown();

        assertEquals(
                numberOfUsers,
                sequenceNumbers.size()
        );
    }
    @Test
void sameUserJoiningConcurrentlyShouldReceiveSameSequence()
        throws Exception {

    Drop drop = dropService.createDrop("Idempotency-Test", 100);

    int numberOfRequests = 50;

    ExecutorService executor =
            Executors.newFixedThreadPool(20);

    CountDownLatch startLatch =
            new CountDownLatch(1);

    List<Future<Long>> futures =
            new ArrayList<>();

    for (int i = 0; i < numberOfRequests; i++) {

        futures.add(
                executor.submit(() -> {
                    startLatch.await();

                    return dropService
                            .joinDrop(
                                    drop.getId(),
                                    "USER-A"
                            )
                            .getSequenceNumber();
                })
        );
    }

    startLatch.countDown();

    Set<Long> sequenceNumbers = new HashSet<>();

    for (Future<Long> future : futures) {
        sequenceNumbers.add(future.get());
    }

    executor.shutdown();

    assertEquals(1, sequenceNumbers.size());
}
@Test
void allocationShouldRespectQueueOrder() {

    Drop drop = dropService.createDrop(
            "Fair-Allocation-Test",
            2
    );

    dropService.joinDrop(
            drop.getId(),
            "USER-A"
    );

    dropService.joinDrop(
            drop.getId(),
            "USER-B"
    );

    dropService.joinDrop(
            drop.getId(),
            "USER-C"
    );

    AllocationResult firstResult =
            dropService.allocateNext(
                    drop.getId()
            );

    AllocationResult secondResult =
            dropService.allocateNext(
                    drop.getId()
            );

    AllocationResult thirdResult =
            dropService.allocateNext(
                    drop.getId()
            );

    assertEquals(
            AllocationStatus.ALLOCATED,
            firstResult.getStatus()
    );

    assertEquals(
            "USER-A",
            firstResult.getQueueEntry().orElseThrow().getUserId()
    );

    assertEquals(
            1,
            firstResult.getQueueEntry()
        .orElseThrow()
        .getSequenceNumber()
    );

    assertEquals(
            AllocationStatus.ALLOCATED,
            secondResult.getStatus()
    );

    assertEquals(
        "USER-B",
        secondResult.getQueueEntry()
                .orElseThrow()
                .getUserId()
);

assertEquals(
        2,
        secondResult.getQueueEntry()
                .orElseThrow()
                .getSequenceNumber()
);

    assertEquals(
            AllocationStatus.SOLD_OUT,
            thirdResult.getStatus()
    );
}
@Test
void concurrentAllocationShouldNotOversellAndShouldRespectQueueOrder()
        throws Exception {

    Drop drop = dropService.createDrop(
            "Concurrent-Fair-Allocation-Test",
            2
    );

    dropService.joinDrop(
            drop.getId(),
            "USER-A"
    );

    dropService.joinDrop(
            drop.getId(),
            "USER-B"
    );

    dropService.joinDrop(
            drop.getId(),
            "USER-C"
    );

    int numberOfRequests = 10;

    ExecutorService executor =
            Executors.newFixedThreadPool(10);

    CountDownLatch startLatch =
            new CountDownLatch(1);

    List<Future<AllocationResult>> futures =
            new ArrayList<>();

    for (int i = 0;
         i < numberOfRequests;
         i++) {

        futures.add(
                executor.submit(() -> {

                    startLatch.await();

                    return dropService
                            .allocateNext(
                                    drop.getId()
                            );
                })
        );
    }

    startLatch.countDown();

    Set<String> allocatedUsers =
            new HashSet<>();

    for (Future<AllocationResult> future : futures) {

        AllocationResult result =
                future.get();

        if (result.getStatus() == AllocationStatus.ALLOCATED) {
    allocatedUsers.add(
            result.getQueueEntry()
                    .orElseThrow()
                    .getUserId()
    );
}
}

    executor.shutdown();

    assertEquals(
            Set.of(
                    "USER-A",
                    "USER-B"
            ),
            allocatedUsers
    );

    Drop updatedDrop =
            dropService.getDrop(
                    drop.getId()
            );

    assertEquals(
            2,
            updatedDrop.getAllocatedCount()
    );
}
@Test
void allocationShouldReturnNoEligibleUserWhenQueueIsEmpty() {

    Drop drop = dropService.createDrop(
            "Empty-Queue-Test",
            5
    );

    AllocationResult result =
            dropService.allocateNext(
                    drop.getId()
            );

    assertEquals(
            AllocationStatus.NO_ELIGIBLE_USER,
            result.getStatus()
    );
}
@Test
void allocationShouldReturnNoEligibleUserWhenEveryoneIsAlreadyAllocated() {

    Drop drop = dropService.createDrop(
            "Everyone-Allocated-Test",
            5
    );

    dropService.joinDrop(
            drop.getId(),
            "USER-A"
    );

    dropService.joinDrop(
            drop.getId(),
            "USER-B"
    );

    AllocationResult first =
            dropService.allocateNext(
                    drop.getId()
            );

    AllocationResult second =
            dropService.allocateNext(
                    drop.getId()
            );

    AllocationResult third =
            dropService.allocateNext(
                    drop.getId()
            );

    assertEquals(
            AllocationStatus.ALLOCATED,
            first.getStatus()
    );

    assertEquals(
            AllocationStatus.ALLOCATED,
            second.getStatus()
    );

    assertEquals(
            AllocationStatus.NO_ELIGIBLE_USER,
            third.getStatus()
    );
}
}