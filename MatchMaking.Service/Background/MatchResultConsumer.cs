using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using MatchMaking.Shared.Kafka;
using MatchMaking.Service.Services;

namespace MatchMaking.Service.Background;

public class MatchResultConsumer(
    IConfiguration config, 
    IMatchStorageService storageService, 
    ILogger<MatchResultConsumer> logger) : BaseKafkaConsumer<MatchFound>(
        config,
        logger,
        QueueConstants.CompleteTopic,
        KafkaConsumerGroups.ServiceGroup)
{
    protected override async Task ProcessMessageAsync(MatchFound matchFound, CancellationToken stoppingToken)
    {
        await storageService.SaveMatchFoundAsync(matchFound);
    }
}