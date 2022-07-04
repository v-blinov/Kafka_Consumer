using System.Text.Json;
using Confluent.Kafka;

namespace SomeService.Models;

internal sealed class CustomKafkaSerializer<T> : ISerializer<T>, IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
        => JsonSerializer.Deserialize<T>(data);

    public byte[] Serialize(T data, SerializationContext context) 
        => JsonSerializer.SerializeToUtf8Bytes(data);
}
