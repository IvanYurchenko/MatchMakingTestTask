using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services;

public interface IMatchMessagingService
{
    Task SendMatchRequestAsync(MatchRequest request);
}
