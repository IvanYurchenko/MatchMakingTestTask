using Confluent.Kafka;
using MatchMaking.Service.Background;
using MatchMaking.Service.Endpoints;
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
var kafkaConfig = new ProducerConfig
{
    BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "kafka:9092"
};
builder.Services.AddSingleton(kafkaConfig);
builder.Services.AddSingleton<IProducer<Null, string>>(sp => 
    new ProducerBuilder<Null, string>(sp.GetRequiredService<ProducerConfig>()).Build());

// 4. Rate Limiter (Fixed window: 1 request per 100ms)
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

// 6. Endpoints
app.MapMatchEndpoints();

app.Run();