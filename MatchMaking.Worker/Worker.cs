using Confluent.Kafka;
using MatchMaking.Shared;
using StackExchange.Redis;
using System.Text.Json;
using System.IO;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;

namespace MatchMaking.Worker;

public class Worker(
    IConfiguration config, 
    IConnectionMultiplexer redis, 
    ILogger<Worker> logger) : BackgroundService
{
    private readonly int _matchSize = config.GetValue<int>("MatchSettings:MatchSize", 3);

    // Load Lua script from Scripts/matchmaking.lua at startup
    private readonly string _matchLuaScript = LoadLuaScript();

    private static string LoadLuaScript()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Scripts", "matchmaking.lua");
        return File.ReadAllText(path);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "kafka:9092",
            GroupId = KafkaConsumerGroups.WorkerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
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
                logger.LogError(ex, "Error processing matchmaking request");
            }
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