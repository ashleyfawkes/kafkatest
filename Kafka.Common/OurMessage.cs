namespace Kafka.Common
{
    public class OurMessage
    {
        public OurMessage(string k, long t, double v)
        {
            key = key;
            value.time = "t" + t;
            value.value = v;
        }
        public string key { get; set; }

        public OurMessageTimeValue value { get; set; } = new OurMessageTimeValue();
    }
}
