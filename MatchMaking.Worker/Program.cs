using MatchMaking.Worker;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

// 1. Register Redis
// We get the connection string from configuration (Environment variables or appsettings)
var redisConn = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(
ConnectionMultiplexer.Connect(redisConn));

// 2. Register the Worker Service
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();