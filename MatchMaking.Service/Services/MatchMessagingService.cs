using Confluent.Kafka;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using System.Text.Json;

namespace MatchMaking.Service.Services;

public class MatchMessagingService(IProducer<Null, string> producer, ILogger<MatchMessagingService> logger) : IMatchMessagingService
{
    public async Task SendMatchRequestAsync(MatchRequest request)
    {
        try
        {
            var json = JsonSerializer.Serialize(request);
            await producer.ProduceAsync(QueueConstants.RequestTopic, new Message<Null, string> { Value = json });
            logger.LogInformation("Match request queued for User: {UserId}", request.UserId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to queue match request for User: {UserId}", request.UserId);
            throw; // Let the caller handle the specific response
        }
    }
}
