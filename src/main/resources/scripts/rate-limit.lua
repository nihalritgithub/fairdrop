-- KEYS[1]: counter key for one user and drop
-- ARGV[1]: window length in seconds
local current = redis.call('INCR', KEYS[1])

-- Also repair a counter that has lost its expiry, so a user can never be locked out permanently.
if current == 1 or redis.call('TTL', KEYS[1]) < 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end

return current
