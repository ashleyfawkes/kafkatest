using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using ExcelDna.Integration;
using ExcelDna.Integration.Rtd;

namespace KafkaExcelDNA
{
    [ComVisible(true)]                   
    [ProgId(RtdKafkaServer.ServerProgId)]
    public class RtdKafkaServer : ExcelRtdServer
    {
        public const string ServerProgId = "RtdKafka.RtdKafkaServer";

        private List<Topic> _topics;
        private Dictionary<Topic, Tuple<IConsumer<Null, string>, CancellationTokenSource>> _topicKafkaConsumers;

        protected override bool ServerStart()
        {
            _topics = new List<Topic>();
            _topicKafkaConsumers = new Dictionary<Topic, Tuple<IConsumer<Null, string>, CancellationTokenSource>>();
            return true;
        }

        protected override void ServerTerminate()
        {
            foreach (var t in _topicKafkaConsumers)
            {
                t.Value.Item2.Cancel();
                t.Value.Item1.Close();
            }
            _topicKafkaConsumers.Clear();
            _topics.Clear();
        }

        protected override object ConnectData(Topic topic, IList<string> topicInfo, ref bool newValues)
        {
            if (topicInfo.Count > 1)
            {
                _topics.Add(topic);
                ConsumeKafka(topic, topicInfo[0], topicInfo[1]);
            }
            return "Waiting...";
        }


        public void ConsumeKafka(Topic topic, string host, string kafkaTopic)
        {
            var tokenSource = new CancellationTokenSource();
            var config = new ConsumerConfig
            {
                BootstrapServers = host,
                GroupId = "foo",
                EnableAutoCommit = true,
                AutoOffsetReset = AutoOffsetReset.Latest
            };
            var consumer = new ConsumerBuilder<Null, string>(config)
                .SetPartitionsAssignedHandler((c, tps) =>
                {
                    var partitionOffsets = c.Committed(tps, TimeSpan.FromSeconds(10));
                    var watermarkOffsets = tps.Select(tp => c.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(10)));
                    var offsets = watermarkOffsets.Zip(partitionOffsets, (watermarkOffset, topicPartitionOffset) =>
                    {
                        if (topicPartitionOffset.Offset.IsSpecial || watermarkOffset.High.IsSpecial)
                        {
                            return topicPartitionOffset.Offset;
                        }

                        var lastTopicOffset = watermarkOffset.High - 1;
                        var lastCommittedOffset = topicPartitionOffset.Offset - 1;

                        if (lastTopicOffset == 0)
                        {
                            return new Offset(0);
                        }
                        if (lastCommittedOffset == lastTopicOffset)
                        {
                            return new Offset(lastCommittedOffset);
                        }
                        return new Offset(lastCommittedOffset + 1);
                    });

                    return tps.Zip(offsets, (partition, offset) => new TopicPartitionOffset(partition, offset));
                })
                .Build();
            _topicKafkaConsumers.Add(topic, new Tuple<IConsumer<Null, string>, CancellationTokenSource>(consumer, tokenSource));
            consumer.Subscribe(kafkaTopic);

            Task.Run(() => ConsumeMessages(topic, consumer, tokenSource.Token));
        }


        public static void ConsumeMessages(Topic topic, IConsumer<Null, string> consumer, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer?.Consume(cancellationToken);
                    var value = consumeResult?.Message?.Value;
                    if (value!= null)
                        topic.UpdateValue(value.Trim());
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                }
            };
            consumer.Close();
        }

        protected override void DisconnectData(Topic topic)
        {
            _topicKafkaConsumers[topic].Item2.Cancel();
            _topicKafkaConsumers.Remove(topic);
            _topics.Remove(topic);
        }
    }

    public static class RtdKafka
    {
        [ExcelFunction(Description = "KafkaRTD Provides updates from Kafka")]
        public static object KafkaRTD(string host, string topic)
        {
            return XlCall.RTD(RtdKafkaServer.ServerProgId, "", host, topic);
        }
    }
}
