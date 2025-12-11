-- Keys: { 'matchmaking:lobby', 'matchmaking:set' }
-- Argv: { userId, matchSize }

local lobbyKey = KEYS[1]
local setKey = KEYS[2]
local userId = ARGV[1]
local matchSize = tonumber(ARGV[2])

-- 1. Add User (Idempotent check)
if redis.call('SISMEMBER', setKey, userId) == 0 then
    redis.call('RPUSH', lobbyKey, userId)
    redis.call('SADD', setKey, userId)
end

-- 2. Check for Match
if redis.call('LLEN', lobbyKey) >= matchSize then
    local players = redis.call('LPOP', lobbyKey, matchSize)
    -- Clean up the Set
    for i, player in ipairs(players) do
        redis.call('SREM', setKey, player)
    end
    return players
end
return nil
