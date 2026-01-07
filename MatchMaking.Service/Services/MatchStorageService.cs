using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using StackExchange.Redis;
using System.Text.Json;

namespace MatchMaking.Service.Services;

public class MatchStorageService(IConnectionMultiplexer redis, ILogger<MatchStorageService> logger) : IMatchStorageService
{
    private static readonly TimeSpan MatchTtl = TimeSpan.FromMinutes(10);

    public async Task SaveMatchFoundAsync(MatchFound matchFound)
    {
        var db = redis.GetDatabase();
        logger.LogInformation("Match formed: {MatchId}. Saving for users.", matchFound.MatchId);

        var messageValue = JsonSerializer.Serialize(matchFound);
        foreach (var user in matchFound.UserIds)
        {
            await db.StringSetAsync(
                RedisKeys.GetUserMatchKey(user),
                messageValue,
                MatchTtl);
        }
    }

    public async Task<MatchFound?> GetMatchFoundAsync(string userId)
    {
        var db = redis.GetDatabase();
        var matchJson = await db.StringGetAsync(RedisKeys.GetUserMatchKey(userId));

        if (matchJson.IsNullOrEmpty)
            return null;

        return JsonSerializer.Deserialize<MatchFound>(matchJson!);
    }
}
