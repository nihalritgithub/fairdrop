package com.fairdrop.cache;

import com.fairdrop.model.Drop;

import java.util.Optional;

/**
 * Cache-aside store for drop snapshots. PostgreSQL stays authoritative: cached drops are only
 * served to readers and are never used for allocation decisions. Implementations must fail open,
 * i.e. treat any cache problem as a miss rather than an error.
 */
public interface DropCache {

    Optional<Drop> find(String dropId);

    void put(Drop drop);

    void evict(String dropId);
}
