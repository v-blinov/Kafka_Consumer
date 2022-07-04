using System.Runtime.CompilerServices;
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
        GroupId = "Group_6", 
        AutoOffsetReset = AutoOffsetReset.Earliest, 
        EnableAutoCommit = false
    };

    private const int MessagesBatchMaxSize = 6;
    private static readonly TimeSpan LingerMs = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan ProducerDelay = TimeSpan.FromMilliseconds(300);
    private static readonly TimeSpan ConsumerTimeout = TimeSpan.FromMilliseconds(100);

    private delegate IEnumerable<TopicPartitionOffset> ProcessMessageDelegate<TKey, TValue>(IReadOnlyCollection<KafkaMessageWrap<TKey, TValue>> messages, int consumerNumber, CancellationToken cancellationToken);

    private static readonly Random Random = new();

    
    public static async Task Main()
    {
        var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;

        // await ProduceMessages(cancellationToken);
        
        // await Task.Run(async () => await ConsumeMessages(1, ProcessMessage, cancellationToken), cancellationToken);
        var consumer1Task = Task.Run(async () => await ConsumeMessages(1, ProcessMessage, cancellationToken), cancellationToken);
        var consumer2Task = Task.Run(async () => await ConsumeMessages(2, ProcessMessage, cancellationToken), cancellationToken);
        var consumer3Task = Task.Run(async () => await ConsumeMessages(3, ProcessMessage, cancellationToken), cancellationToken);
        
        await Task.WhenAll(consumer1Task, consumer2Task, consumer3Task).ConfigureAwait(false);
        Console.ReadLine();
    }

    private static async Task ProduceMessages(CancellationToken cancellationToken)
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
    
    private static async Task ConsumeMessages(int consumerNumber, ProcessMessageDelegate<string, Person> processMessage, CancellationToken cancellationToken)
    {
        var redis = await ConnectionMultiplexer.ConnectAsync(RedisHost);
        var redisDb = redis.GetDatabase();
        
        using var consumer = new ConsumerBuilder<string, Person>(ConsumerConfig)
                       .SetValueDeserializer(new CustomKafkaSerializer<Person>())
                       .Build();

        consumer.Subscribe(PersonTopic);

        var lastConsumeBatchIsSuccessfully = true;
        IReadOnlyCollection<KafkaMessageWrap<string, Person>> messages = new List<KafkaMessageWrap<string, Person>>();

        while(!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if(lastConsumeBatchIsSuccessfully)
                {
                    // Принять пачку сообщений из топика
                    messages = consumer.ConsumeBatch(cancellationToken);
                    if(!messages.Any())
                        continue;
                }

                // Выбрать еще не обработанные
                var redisKeys = messages.Select(p => new MessageRedisKey { ConsumerGroupId = consumer.MemberId, TopicPartitionOffset = p.Id });
                var unprocessedMessageKeys = FilterUnprocessedMessages(redisDb, redisKeys);

                // Обработать
                var unprocessedMessages = messages.Where(p => unprocessedMessageKeys.Contains(p.Id)).ToArray();
                var successfulProcessed = processMessage(unprocessedMessages, consumerNumber, cancellationToken);

                // Сохранить в redis свеже-обработанные
                var freshProcessedRedisKeys = successfulProcessed.Select(p => new MessageRedisKey { ConsumerGroupId = consumer.MemberId, TopicPartitionOffset = p });
                SetProcessedMessageKeys(redisDb, freshProcessedRedisKeys);

                if(successfulProcessed.Count() == unprocessedMessageKeys.Count())   //TODO: по-другому отслеживать наличие ошибок
                {
                    // Если все прошло без ошибок, зафиксировать offset
                    consumer.Commit(messages.Select(p => p.Id).ToArray());
                    lastConsumeBatchIsSuccessfully = true;
                } 
                else
                {
                    lastConsumeBatchIsSuccessfully = false;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }
            catch(Exception ex)
            {
                Console.WriteLine($"[Error] : {ex.Message}");
                lastConsumeBatchIsSuccessfully = false;
            }
        }
        
        consumer.Close();
    }

    private static IReadOnlyCollection<KafkaMessageWrap<TKey, TValue>> ConsumeBatch<TKey, TValue>(this IConsumer<TKey, TValue> consumer, CancellationToken cancellationToken)
    {
        var lastConsumeIsSuccessfully = true;
        var deadline = DateTime.UtcNow.Add(LingerMs);

        ConsumeResult<TKey, TValue>? message = null;
        Queue<KafkaMessageWrap<TKey, TValue>> messages = new();

        bool ConfigureBatch(IReadOnlyCollection<KafkaMessageWrap<TKey, TValue>> kafkaMessageWraps, DateTime deadline)
            => DateTime.UtcNow >= deadline && kafkaMessageWraps.Any() || kafkaMessageWraps.Count >= MessagesBatchMaxSize;
        
        while(!cancellationToken.IsCancellationRequested)
        {
            if (lastConsumeIsSuccessfully) 
                message = consumer.Consume(ConsumerTimeout);

            try
            {
                if(message is null)
                    continue;

                messages.Enqueue(new KafkaMessageWrap<TKey, TValue>
                {
                    Id = message.TopicPartitionOffset, 
                    Message = message.Message
                });
                
                lastConsumeIsSuccessfully = true;

                Console.WriteLine($"[Info] --> ConsumerName: {consumer.Name} | Consume {message.TopicPartitionOffset} => key : {message.Message.Key}, value: {{ {message.Message.Value} }} => InBatchPosition: {messages.Count}");

                if(ConfigureBatch(messages, deadline))
                    break;
            }
            catch(Exception ex)
            {
                lastConsumeIsSuccessfully = false;
                Console.WriteLine($"[Error] ::: ConsumerName: {consumer.Name} | consume messages error ({message?.Topic}:{message?.Partition}:{message?.Offset}): {ex.Message}");
            }
        }
        
        return messages;
    }

    private static IEnumerable<TopicPartitionOffset> FilterUnprocessedMessages(IDatabase redis, IEnumerable<MessageRedisKey> messageKeys)
    {
        var keys = messageKeys.ToArray();
        var redisKeys = keys.Select(p => (RedisKey)p.ToString()).ToArray();
        var redisValues = redis.StringGet(redisKeys);

        var unsavedKeys = new List<TopicPartitionOffset>(redisValues.Count(p => !p.HasValue || p.IsNull));
        
        for(var i = 0; i < redisKeys.Length; i++)
        {
            if (!redisValues[i].HasValue || redisValues[i].IsNull)
                unsavedKeys.Add(keys[i].TopicPartitionOffset);
            else
                Console.WriteLine($"[INFO] --> redis already has TopicPartitionOffset: {keys[i].TopicPartitionOffset} => this message is processed already.");
        }

        return unsavedKeys;
    }

    private static void SetProcessedMessageKeys(IDatabase redis, IEnumerable<MessageRedisKey> freshProcessedRedisKeys)
    {
        var keys = freshProcessedRedisKeys.ToArray();
        var redisKeys = keys.Select(p => new KeyValuePair<RedisKey,RedisValue>((RedisKey)p.ToString(), true)).ToArray();
        
        redis.StringSet(redisKeys);
    }

    private static IEnumerable<TopicPartitionOffset> ProcessMessage<TKey, TValue>(IReadOnlyCollection<KafkaMessageWrap<TKey, TValue>> messages, int consumerNumber, CancellationToken cancellationToken)
    {
        var successfulProcessed = new List<TopicPartitionOffset>();
        
        foreach(var message in messages)
        {
            if(cancellationToken.IsCancellationRequested)
                return successfulProcessed;

            try
            {
                // Processing logic
                
                if(Random.Next(10) % 9 == 0)
                    throw new Exception($"ConsumerNumber: {consumerNumber} | Random custom error");
                
                Console.WriteLine($"[Info] --> ConsumerNumber: {consumerNumber} | Success process {message.Id.ToString()}");
                successfulProcessed.Add(message.Id);
            }
            catch(Exception ex)
            {   
                Console.WriteLine($"[Error] --> ConsumerNumber: {consumerNumber} | Error process {message.Id.ToString()} | ({ex.Message})");
            }
        }

        return successfulProcessed;
    }
}
