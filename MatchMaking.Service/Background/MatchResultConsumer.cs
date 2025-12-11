using Confluent.Kafka;
using MatchMaking.Shared;
using StackExchange.Redis;
using System.Text.Json;

namespace MatchMaking.Service.Background;

public class MatchResultConsumer(
    IConfiguration config, 
    IConnectionMultiplexer redis, 
    ILogger<MatchResultConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "kafka:9092",
            GroupId = "matchmaking-service-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        consumer.Subscribe(QueueConstants.CompleteTopic);

        var db = redis.GetDatabase();

        await Task.Yield(); // Release blocking on startup

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);
                var matchFound = JsonSerializer.Deserialize<MatchFound>(consumeResult.Message.Value);

                if (matchFound is not null)
                {
                    logger.LogInformation("Match formed: {MatchId}. Saving for users.", matchFound.MatchId);

                    // Save match result for each user in Redis so they can poll it
                    foreach (var user in matchFound.UserIds)
                    {
                        // Expiry: 10 minutes to keep Redis clean
                        await db.StringSetAsync(
                            $"user:{user}:match", 
                            consumeResult.Message.Value, 
                            TimeSpan.FromMinutes(10));
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error consuming match result.");
            }
        }
        consumer.Close();
    }
}