using Kafka.Common;
using System;
using System.Configuration;
using System.Data.SqlClient;

namespace KafkaSubscriberExample
{
    public class MessageRepository
    {
        private static SqlConnection _sqlConnection;
        private static SqlCommand _sqlCommand;
        public MessageRepository()
        {
            _sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["mydb"].ConnectionString);
            _sqlCommand = _sqlConnection.CreateCommand();
            _sqlCommand.CommandText = "INSERT INTO MyTable (key, time, value) SELECT @key, @time, @value";
            _sqlCommand.Parameters.AddWithValue("key", "");
            _sqlCommand.Parameters.AddWithValue("time", 0);
            _sqlCommand.Parameters.AddWithValue("value", 0.0D);
        }

        public void StoreMessage(OurMessage message)
        {
            //store in DB
            if (_sqlConnection.State != System.Data.ConnectionState.Open)
            {
                //_sqlConnection.Open(); //no db to connect to - just demonstrating - would probably use Dapper in reality
            }
            _sqlCommand.Parameters["key"].Value = message.key;
            _sqlCommand.Parameters["time"].Value = message.value.time;
            _sqlCommand.Parameters["value"].Value = message.value.value;

            //_sqlCommand.ExecuteNonQuery(); //no db to connect to - just demonstrating - would probably use Dapper in reality
            Console.WriteLine(_sqlCommand.CommandText + $" WHERE @key={message.key}, @time=t{message.value.time}, @value={message.value.value}");
        }
    }
}
