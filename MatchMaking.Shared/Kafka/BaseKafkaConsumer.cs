using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace MatchMaking.Shared.Kafka;

public abstract class BaseKafkaConsumer<TValue>(
    IConfiguration config,
    ILogger logger,
    string topic,
    string groupId) : BackgroundService
{
    protected abstract Task ProcessMessageAsync(TValue message, CancellationToken stoppingToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "kafka:9092",
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        logger.LogInformation("Started consuming topic: {Topic}", topic);

        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                if (result?.Message?.Value == null) continue;

                var message = JsonSerializer.Deserialize<TValue>(result.Message.Value);
                if (message != null)
                {
                    await ProcessMessageAsync(message, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error processing Kafka message from topic: {Topic}", topic);
            }
        }

        consumer.Close();
    }
}
