-- One row per (consumer, event) whose side effects have been applied; makes Kafka consumers idempotent.
CREATE TABLE processed_events (
    consumer_name VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT processed_events_pkey
        PRIMARY KEY (consumer_name, event_id)
);


CREATE TABLE allocation_audit_log (
    event_id UUID NOT NULL,
    allocation_id VARCHAR(255) NOT NULL,
    drop_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    allocated_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT allocation_audit_log_pkey
        PRIMARY KEY (event_id),

    CONSTRAINT uk_allocation_audit_allocation
        UNIQUE (allocation_id)
);
