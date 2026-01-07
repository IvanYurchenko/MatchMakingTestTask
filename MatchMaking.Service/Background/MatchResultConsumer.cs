using Confluent.Kafka;
using StackExchange.Redis;
using System.Text.Json;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;

using MatchMaking.Shared.Kafka;

namespace MatchMaking.Service.Background;

public class MatchResultConsumer(
    IConfiguration config, 
    IConnectionMultiplexer redis, 
    ILogger<MatchResultConsumer> logger) : BaseKafkaConsumer<MatchFound>(
        config,
        logger,
        QueueConstants.CompleteTopic,
        KafkaConsumerGroups.ServiceGroup)
{
    private static readonly TimeSpan MatchTtl = TimeSpan.FromMinutes(10);

    protected override async Task ProcessMessageAsync(MatchFound matchFound, CancellationToken stoppingToken)
    {
        var db = redis.GetDatabase();
        logger.LogInformation("Match formed: {MatchId}. Saving for users.", matchFound.MatchId);

        // Save match result for each user in Redis so they can poll it
        var messageValue = JsonSerializer.Serialize(matchFound);
        foreach (var user in matchFound.UserIds)
        {
            await db.StringSetAsync(
                RedisKeys.GetUserMatchKey(user),
                messageValue,
                MatchTtl);
        }
    }
}