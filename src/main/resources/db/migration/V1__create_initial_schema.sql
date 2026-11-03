CREATE TABLE drops (
    id VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    status VARCHAR(255),
    total_inventory INTEGER NOT NULL,
    allocated_count INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT drops_pkey
        PRIMARY KEY (id),

    CONSTRAINT drops_status_check
        CHECK (status IN ('UPCOMING', 'OPEN', 'CLOSED'))
);


CREATE TABLE drop_sequences (
    drop_id VARCHAR(255) NOT NULL,
    last_sequence BIGINT NOT NULL,

    CONSTRAINT drop_sequences_pkey
        PRIMARY KEY (drop_id)
);


CREATE TABLE queue_entries (
    id VARCHAR(255) NOT NULL,
    drop_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    user_id VARCHAR(255) NOT NULL,

    CONSTRAINT queue_entries_pkey
        PRIMARY KEY (id),

    CONSTRAINT uk_queue_drop_user
        UNIQUE (drop_id, user_id),

    CONSTRAINT uk_queue_drop_sequence
        UNIQUE (drop_id, sequence_number)
);


CREATE TABLE allocations (
    id VARCHAR(255) NOT NULL,
    drop_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,

    CONSTRAINT allocations_pkey
        PRIMARY KEY (id),

    CONSTRAINT uk_allocation_drop_user
        UNIQUE (drop_id, user_id)
);