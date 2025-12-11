using Confluent.Kafka;
using MatchMaking.Service.Background;
using MatchMaking.Shared;
using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;
using System.Text.Json;
using System.Threading.RateLimiting;

var builder = WebApplication.CreateBuilder(args);

// 1. Configuration & Logging
builder.Services.AddLogging();

// 2. Redis Configuration
var redisConn = builder.Configuration.GetConnectionString("Redis") ?? "redis:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(ConnectionMultiplexer.Connect(redisConn));

// 3. Kafka Producer Configuration
var kafkaConfig = new ProducerConfig { BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "kafka:9092" };
builder.Services.AddSingleton(kafkaConfig);
builder.Services.AddSingleton<IProducer<Null, string>>(sp => 
    new ProducerBuilder<Null, string>(sp.GetRequiredService<ProducerConfig>()).Build());

// 4. Rate Limiter (Token Bucket: 1 request per 100ms)
builder.Services.AddRateLimiter(options =>
{
    options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context =>
    {
        return RateLimitPartition.GetFixedWindowLimiter(
            partitionKey: context.Connection.RemoteIpAddress?.ToString() ?? "anonymous",
            factory: _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = 1,
                Window = TimeSpan.FromMilliseconds(100),
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                QueueLimit = 0
            });
    });
});

// 5. Background Service to consume "MatchComplete" events
builder.Services.AddHostedService<MatchResultConsumer>();

var app = builder.Build();
app.UseRateLimiter();

// --- Endpoints ---

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
    // We store the match result in Redis key: "user:{userId}:match"
    var matchJson = await db.StringGetAsync($"user:{userId}:match");

    if (matchJson.IsNullOrEmpty)
        return Results.NotFound();

    var matchData = JsonSerializer.Deserialize<MatchFound>(matchJson!);
    return Results.Ok(new 
    { 
        matchId = matchData?.MatchId, 
        userIds = matchData?.UserIds 
    });
});

app.Run();