using System;
using System.Collections.Generic;
using System.Net;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Threading;
using Newtonsoft.Json;
using System.Configuration;

namespace Kafka.Common
{
    class Program
    {
        static ProducerConfig _config;
        private static Timer _timer;
        private static IProducer<Null, string> _producer;
        private static Random _random;
        private static long _counter;

        static void Main(string[] args)
        {
            _random = new Random();
            _config = new ProducerConfig
            {
                BootstrapServers = ConfigurationManager.AppSettings["host"],
            };
            _producer = new ProducerBuilder<Null, string>(_config).Build();
            _timer = new Timer(async s => await SendMessages(), null, 500, 500);
            Console.ReadLine();
        }


        public static async Task SendMessages()
        {
            var text = JsonConvert.SerializeObject(new OurMessage($"k{_random.Next(1, 3)}", ++_counter, _random.NextDouble()));
            await _producer.ProduceAsync("kafka-rtd", new Message<Null, string> { Value = text });
            Console.WriteLine(text);
        }
    }

}
