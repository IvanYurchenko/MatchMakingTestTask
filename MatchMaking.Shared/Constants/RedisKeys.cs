namespace MatchMaking.Shared.Constants;

public static class RedisKeys
{
    public const string LobbyListKey = "matchmaking:lobby";
    public const string LobbySetKey = "matchmaking:set";
    public const string LobbyLockKey = "matchmaking:lock";

    public static string GetUserMatchKey(string userId) => $"user:{userId}:match";
}
