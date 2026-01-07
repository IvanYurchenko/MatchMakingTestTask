using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services;

public interface IMatchStorageService
{
    Task SaveMatchFoundAsync(MatchFound matchFound);
    Task<MatchFound?> GetMatchFoundAsync(string userId);
}
