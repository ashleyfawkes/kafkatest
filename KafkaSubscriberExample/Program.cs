using System;
using Kafka.Common;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Threading;
using Newtonsoft.Json;
using System.IO;

namespace KafkaSubscriberExample
{
    class Program
    {
        static ConsumerConfig _config;
        private static IConsumer<Null, string> _consumer;
        private static MessageRepository _messageRepository;
        static void Main(string[] args)
        {
            _config = new ConsumerConfig
            {
                BootstrapServers = System.Configuration.ConfigurationManager.AppSettings["host"],
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            _consumer = new ConsumerBuilder<Null, string>(_config).Build();
            _consumer.Subscribe("kafka-rtd");
            
            _messageRepository = new MessageRepository();

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;
            CancellationToken cancellationToken = new CancellationToken(false);
            Task.Run(() => ConsumeMessages(cancellationToken));
            Console.ReadLine();
            tokenSource.Cancel();
            _consumer.Close();
        }


        public static async Task ConsumeMessages(CancellationToken cancellationToken)
        {
            JsonSerializer serializer = new JsonSerializer();
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);
                    var text = consumeResult.Message.Value?.Trim();
                    if (!string.IsNullOrEmpty(text) && text.StartsWith("{") && text.EndsWith("}"))
                    {
                        var message = serializer.Deserialize<OurMessage>(new JsonTextReader(new StringReader(text)));
                        _messageRepository.StoreMessage(message);
                    }
                }
                catch(Exception exception)
                {
                    Console.WriteLine(exception);
                }
            };
        }

    }
}
