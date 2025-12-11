using MatchMaking.Worker;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

var redisConn = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(
ConnectionMultiplexer.Connect(redisConn));

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();