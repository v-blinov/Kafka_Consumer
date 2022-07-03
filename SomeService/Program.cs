using System.Collections.Immutable;
using System.Text.Json;
using Confluent.Kafka;
using SomeService.Models;
using StackExchange.Redis;

namespace SomeService;

public static class Program
{
    private const string RedisHost = "localhost";
    private const string KafkaHost = "localhost:9092";
    private const string PersonTopic = "person_event";
    private static  readonly ProducerConfig ProducerConfig = new()
    {
        BootstrapServers = KafkaHost
    };
    private static readonly ConsumerConfig ConsumerConfig = new()
    {
        BootstrapServers = KafkaHost, 
        GroupId = "Group_3", 
        AutoOffsetReset = AutoOffsetReset.Earliest, 
        EnableAutoCommit = false
    };

    private static readonly TimeSpan ProducerDelay = TimeSpan.FromMilliseconds(300);
    
    private static readonly int MessagesBatchMaxSize = 6;
    private static readonly TimeSpan LingerMs = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan ConsumerTimeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan FailDelay = TimeSpan.FromMilliseconds(500);

    private static readonly Random Random = new();
    
    
    private class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }
    private class CustomKafkaSerializer<T> : ISerializer<T>, IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => JsonSerializer.Deserialize<T>(data);

        public byte[] Serialize(T data, SerializationContext context) 
            => JsonSerializer.SerializeToUtf8Bytes(data);
    }

    
    public static async Task Main()
    {
        var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;

        // await ProduceMessages(cancellationToken);
        
        var consumer1Task = Task.Run(async () => await ConsumeMessages(1, cancellationToken));
        var consumer2Task = Task.Run(async () => await ConsumeMessages(2, cancellationToken));
        var consumer3Task = Task.Run(async () => await ConsumeMessages(3, cancellationToken));

        await Task.WhenAll(consumer1Task, consumer2Task, consumer3Task);
        Console.ReadLine();
    }

    public static async Task ProduceMessages(CancellationToken cancellationToken)
    {
        var rand = new Random();
        
        using var producer = new ProducerBuilder<string, Person>(ProducerConfig)
                             .SetValueSerializer(new CustomKafkaSerializer<Person>())
                             .Build();

        try
        {
            var i = 0;
            while(!cancellationToken.IsCancellationRequested && i <= 1000)
            {
                var msg = new Message<string, Person>
                {
                    Key = i.ToString(), 
                    Value = new Person
                    {
                        Name = Guid.NewGuid().ToString(), 
                        Age = rand.Next(20, 50)
                    }
                };
                
                producer.Produce(PersonTopic, msg);
                Console.WriteLine($"[Info] : Print ---> message: {{ (key: { msg.Key }, Value:  {{ Name: { msg.Value.Name.ToString()}), Age: { msg.Value.Age }}} }}");

                i++;
                await Task.Delay(ProducerDelay, cancellationToken);
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"[Error] : {ex.Message}");
        }
        finally
        {
            producer.Flush(cancellationToken);
        }
    }
    
    private static async Task ConsumeMessages(int consumerNumber, CancellationToken cancellationToken)
    {
        var redis = await ConnectionMultiplexer.ConnectAsync(RedisHost);
        var redisDb = redis.GetDatabase();
        
        using var consumer = new ConsumerBuilder<string, Person>(ConsumerConfig)
                       .SetValueDeserializer(new CustomKafkaSerializer<Person>())
                       .Build();

        consumer.Subscribe(PersonTopic);

        var lastProcessedIsSuccessfully = true;
        Queue<KafkaMessageWrap<string, Person>> messages = new();
        var deadline = DateTime.UtcNow.Add(LingerMs);
            
        while(!cancellationToken.IsCancellationRequested)
        {
            if(DateTime.UtcNow >= deadline && messages.Any() || messages.Count >= MessagesBatchMaxSize)
            {
                try
                {
                    ProcessMessage(messages.ToImmutableArray(), redisDb, consumerNumber);
                
                    consumer.Commit(messages.Select(p => p.Id));
                        
                    messages.Clear();
                    deadline = DateTime.UtcNow.Add(LingerMs);
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"[Error] ::: ConsumerNumber: {consumerNumber} | Batch processing message failure. BatchMessageFirst:{messages.First().Id}, BatchMessageLast:{messages.Last().Id}. Error: {ex.Message}");
                    await Task.Delay(FailDelay);
                    continue;
                }
            }
            
            ConsumeResult<string, Person> message = null;
            if (lastProcessedIsSuccessfully) 
                message = consumer.Consume(ConsumerTimeout);

            try
            {
                if(message != null)
                {
                    messages.Enqueue(new KafkaMessageWrap<string, Person>
                    {
                        Id = message.TopicPartitionOffset, 
                        Message = message.Message
                    });
                    
                    Console.WriteLine($"[Info] --> ConsumerNumber: {consumerNumber} | Consume {message.TopicPartitionOffset} => key : {message.Message.Key}, value: {{ {message.Message.Value.Name} : {message.Message.Value.Age} }} => InBatchPosition: {messages.Count}");

                    lastProcessedIsSuccessfully = true;
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"[Error] ::: ConsumerNumber: {consumerNumber} | consume messages error ({message.Topic}:{message.Partition}:{message.Offset}): {ex.Message}");
                lastProcessedIsSuccessfully = false;
            }

            // Для разработки
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }
        
        consumer.Close();
    }

    private static void ProcessMessage<TKey, TValue>(IReadOnlyCollection<KafkaMessageWrap<TKey, TValue>> messages, IDatabase redis, int consumerNumber)
    {
        foreach(var message in messages)
        {
            var redisValue = redis.StringGet(message.Id.ToString());
            if(!redisValue.IsNullOrEmpty)
            {
                Console.WriteLine($"[INFO] --> ConsumerNumber: {consumerNumber} | redis already has TopicPartitionOffset: {message.Id} => this message is processed already.");
                continue;
            }

            // Processing logic
            Console.WriteLine($"[Info] --> ConsumerNumber: {consumerNumber} | process {message.Id.ToString()}");
            if(Random.Next(10) % 9 == 0)
                throw new Exception("ConsumerNumber: {consumerNumber} | Random custom error");

            redis.StringSet(key: message.Id.ToString(), value: true);
        }
    }
}
