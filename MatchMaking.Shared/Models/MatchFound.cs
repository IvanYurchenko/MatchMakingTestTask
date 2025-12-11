namespace MatchMaking.Shared.Models;

public sealed record MatchFound(Guid MatchId, List<string> UserIds);
