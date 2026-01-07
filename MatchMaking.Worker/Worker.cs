using Confluent.Kafka;
using MatchMaking.Shared;
using StackExchange.Redis;
using System.Text.Json;
using System.IO;
using System.Linq;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;

using MatchMaking.Shared.Kafka;

namespace MatchMaking.Worker;

public class Worker(
    IConfiguration config,
    IConnectionMultiplexer redis,
    ILogger<Worker> logger) : BaseKafkaConsumer<MatchRequest>(
        config,
        logger,
        QueueConstants.RequestTopic,
        KafkaConsumerGroups.WorkerGroup)
{
    private readonly int _matchSize = config.GetValue<int>("MatchSettings:MatchSize", 3);

    private readonly IProducer<Null, string> _producer = new ProducerBuilder<Null, string>(new ProducerConfig
    {
        BootstrapServers = config["Kafka:BootstrapServers"] ?? "kafka:9092"
    }).Build();

    protected override async Task ProcessMessageAsync(MatchRequest request, CancellationToken stoppingToken)
    {
        if (!string.IsNullOrEmpty(request.UserId))
        {
            var db = redis.GetDatabase();
            await ProcessUserAsync(db, _producer, request.UserId);
        }
    }

    private async Task ProcessUserAsync(IDatabase db, IProducer<Null, string> producer, string userId)
    {
        // 1. Add User (Optimized Idempotent Add)
        // SetAddAsync returns true if the member was added, false if it already exists
        if (await db.SetAddAsync(RedisKeys.LobbySetKey, userId))
        {
            // Push to list and get new length
            var currentQueueSize = await db.ListRightPushAsync(RedisKeys.LobbyListKey, userId);

            // 2. Check for Match - only attempt if we have enough players
            if (currentQueueSize >= _matchSize)
            {
                await TryCreateMatchAsync(db, producer);
            }
        }
    }

    private async Task TryCreateMatchAsync(IDatabase db, IProducer<Null, string> producer)
    {
        var lockValue = Guid.NewGuid().ToString();
        var lockExpiry = TimeSpan.FromSeconds(10);

        // Try to acquire lock to ensure atomic match creation
        // Only one worker should form a match at a time
        if (await db.LockTakeAsync(RedisKeys.LobbyLockKey, lockValue, lockExpiry))
        {
            try
            {
                // Re-verify length inside the lock
                if (await db.ListLengthAsync(RedisKeys.LobbyListKey) >= _matchSize)
                {
                    var players = await db.ListLeftPopAsync(RedisKeys.LobbyListKey, _matchSize);
                    if (players != null && players.Length >= _matchSize)
                    {
                        // Use Batch for cleanup to reduce round-trips
                        var batch = db.CreateBatch();
                        var tasks = players.Select(player => batch.SetRemoveAsync(RedisKeys.LobbySetKey, player)).ToArray();
                        batch.Execute();
                        await Task.WhenAll(tasks);

                        var userIds = players.Select(p => p.ToString()).ToList();
                        await CreateMatchAsync(producer, userIds);
                    }
                }
            }
            finally
            {
                await db.LockReleaseAsync(RedisKeys.LobbyLockKey, lockValue);
            }
        }
    }

    private async Task CreateMatchAsync(IProducer<Null, string> producer, List<string> userIds)
    {
        var matchId = Guid.NewGuid();
        var matchFound = new MatchFound(matchId, userIds);
        var json = JsonSerializer.Serialize(matchFound);

        logger.LogInformation("Match Created! ID: {MatchId}, Users: {Users}", matchId, string.Join(", ", userIds));

        await producer.ProduceAsync(
            QueueConstants.CompleteTopic,
            new Message<Null, string> { Value = json }
        );
    }
}