namespace MatchMaking.Shared;

public record MatchRequest(string UserId);

public record MatchFound(Guid MatchId, List<string> UserIds);

public static class QueueConstants
{
    public const string RequestTopic = "matchmaking.request";
    public const string CompleteTopic = "matchmaking.complete";
    public const string RedisLobbyKey = "matchmaking:lobby";
}