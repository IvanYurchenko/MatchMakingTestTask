using Confluent.Kafka;
using MatchMaking.Shared;
using StackExchange.Redis;
using System.Text.Json;

namespace MatchMaking.Worker;

public class Worker(
    IConfiguration config, 
    IConnectionMultiplexer redis, 
    ILogger<Worker> logger) : BackgroundService
{
    private readonly int _matchSize = config.GetValue<int>("MatchSettings:MatchSize", 3);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "kafka:9092",
            GroupId = "matchmaking-worker-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true // For simplicity in this test task
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "kafka:9092"
        };

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        
        consumer.Subscribe(QueueConstants.RequestTopic);
        var db = redis.GetDatabase();

        logger.LogInformation("Worker started. Match Size required: {MatchSize}", _matchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                var request = JsonSerializer.Deserialize<MatchRequest>(result.Message.Value);

                if (request is not null && !string.IsNullOrEmpty(request.UserId))
                {
                    await ProcessUserAsync(db, producer, request.UserId);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing matchmaking request.");
            }
        }
    }

    private async Task ProcessUserAsync(IDatabase db, IProducer<Null, string> producer, string userId)
    {
        // LUA SCRIPT: 
        // 1. Check if user is already in the list (using a helper Set for O(1) lookup).
        // 2. If not, Add to List and Set.
        // 3. Check if List >= MatchSize.
        // 4. If yes, Pop MatchSize items and Remove them from the Set.
        var script = @"
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
    ";

        var result = await db.ScriptEvaluateAsync(script, 
        keys: new RedisKey[] { QueueConstants.RedisLobbyKey, "matchmaking:set" },
        values: new RedisValue[] { userId, _matchSize }
        );

        if (!result.IsNull)
        {
            string[] userIds = (string[])result!;
        
            var matchId = Guid.NewGuid();
            var matchFound = new MatchFound(matchId, userIds.ToList());
            var json = JsonSerializer.Serialize(matchFound);

            logger.LogInformation("Match Created! ID: {MatchId}, Users: {Users}", matchId, string.Join(", ", userIds));

            await producer.ProduceAsync(QueueConstants.CompleteTopic, 
            new Message<Null, string> { Value = json });
        }
    }
}