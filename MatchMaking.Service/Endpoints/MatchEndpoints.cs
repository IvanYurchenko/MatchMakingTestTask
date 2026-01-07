using MatchMaking.Shared.Models;
using MatchMaking.Service.Services;
using Microsoft.AspNetCore.Mvc;

namespace MatchMaking.Service.Endpoints;

public static class MatchEndpoints
{
    public static void MapMatchEndpoints(this IEndpointRouteBuilder app)
    {
        // POST /match/search
        app.MapPost("/match/search", async (
            [FromBody] MatchRequest request,
            IMatchMessagingService messagingService) =>
        {
            if (string.IsNullOrWhiteSpace(request.UserId))
                return Results.BadRequest("UserId is required.");

            try
            {
                await messagingService.SendMatchRequestAsync(request);
                return Results.NoContent();
            }
            catch (Exception)
            {
                return Results.Problem("Internal system error");
            }
        });

        // GET /match/status?userId=...
        app.MapGet("/match/status", async (string userId, IMatchStorageService storageService) =>
        {
            var matchData = await storageService.GetMatchFoundAsync(userId);

            if (matchData == null)
                return Results.NotFound();

            return Results.Ok(new
            {
                matchId = matchData.MatchId,
                userIds = matchData.UserIds
            });
        });
    }
}
