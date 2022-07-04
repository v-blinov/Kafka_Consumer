using Confluent.Kafka;

namespace SomeService.Models;

public record KafkaMessageWrap<TKey, TValue>
{
    public TopicPartitionOffset Id { get; init; } = null!;
    public Message<TKey, TValue> Message { get; init; } = null!;
}
