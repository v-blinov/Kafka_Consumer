using Confluent.Kafka;

namespace SomeService.Models;

public record MessageRedisKey
{
    public TopicPartitionOffset TopicPartitionOffset { get; set; } = null!;
    public string ConsumerGroupId { get; set; } = null!;

    public override string ToString()
        => $"{ConsumerGroupId}-{TopicPartitionOffset}";
}
