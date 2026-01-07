using Confluent.Kafka;
using MatchMaking.Shared;
using StackExchange.Redis;
using System.Text.Json;
using System.IO;
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

    // Load Lua script from Scripts/matchmaking.lua at startup
    private readonly string _matchLuaScript = LoadLuaScript();

    private static string LoadLuaScript()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Scripts", "matchmaking.lua");
        return File.ReadAllText(path);
    }

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
        var result = await db.ScriptEvaluateAsync(
            _matchLuaScript,
            keys: new RedisKey[] { RedisKeys.LobbyListKey, RedisKeys.LobbySetKey },
            values: new RedisValue[] { userId, _matchSize }
        );

        if (!result.IsNull)
        {
            string[] userIds = (string[])result!;
        
            var matchId = Guid.NewGuid();
            var matchFound = new MatchFound(matchId, userIds.ToList());
            var json = JsonSerializer.Serialize(matchFound);

            logger.LogInformation("Match Created! ID: {MatchId}, Users: {Users}", matchId, string.Join(", ", userIds));

            await producer.ProduceAsync(
                QueueConstants.CompleteTopic,
                new Message<Null, string> { Value = json }
            );
        }
    }
}