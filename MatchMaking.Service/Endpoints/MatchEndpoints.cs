using Confluent.Kafka;
using MatchMaking.Shared;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;
using System.Text.Json;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Endpoints;

public static class MatchEndpoints
{
    public static void MapMatchEndpoints(this IEndpointRouteBuilder app)
    {
        // POST /match/search
        app.MapPost("/match/search", async (
            [FromBody] MatchRequest request,
            IProducer<Null, string> producer,
            ILogger<Program> logger) =>
        {
            if (string.IsNullOrWhiteSpace(request.UserId))
                return Results.BadRequest("UserId is required.");

            try
            {
                var json = JsonSerializer.Serialize(request);
                await producer.ProduceAsync(QueueConstants.RequestTopic, new Message<Null, string> { Value = json });
                logger.LogInformation("Match request queued for User: {UserId}", request.UserId);
                return Results.NoContent();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to queue match request for User: {UserId}", request.UserId);
                return Results.Problem("Internal system error");
            }
        });

        // GET /match/status?userId=...
        app.MapGet("/match/status", async (string userId, IConnectionMultiplexer redis) =>
        {
            var db = redis.GetDatabase();
            var matchJson = await db.StringGetAsync(RedisKeys.GetUserMatchKey(userId));

            if (matchJson.IsNullOrEmpty)
                return Results.NotFound();

            var matchData = JsonSerializer.Deserialize<MatchFound>(matchJson!);
            return Results.Ok(new
            {
                matchId = matchData?.MatchId,
                userIds = matchData?.UserIds
            });
        });
    }
}
