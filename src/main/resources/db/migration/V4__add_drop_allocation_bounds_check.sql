-- Database-level backstop against overselling, independent of application locking.
-- NOT VALID enforces the rule for every new insert/update without failing on historical rows.
ALTER TABLE drops
    ADD CONSTRAINT drops_allocated_count_bounds_check
        CHECK (allocated_count >= 0 AND allocated_count <= total_inventory)
        NOT VALID;
