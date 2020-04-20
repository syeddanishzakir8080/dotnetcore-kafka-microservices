namespace KafkaEngine
{
    using Confluent.Kafka;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public class ProducerWrapper<T>
    {
        private string _topicName;
        private Producer<string,T> _producer;
        private ProducerConfig _config;
        private static readonly Random rand = new Random();

        public ProducerWrapper(ProducerConfig config,string topicName)
        {
            this._topicName = topicName;
            this._config = config;
            this._producer = new Producer<string,T>(this._config);
            
            //setting error handler via delegate
            this._producer.OnError += (_,e)=>{
                Console.WriteLine("Exception:"+e);
            };
        }
        public async Task writeMessage(T message){
            var dr = await this._producer.ProduceAsync(this._topicName, new Message<string, T>()
                        {
                            Key = rand.Next(5).ToString(),
                            Value = message
                        });
            Console.WriteLine($"KAFKA => Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            return;
        }
    }
}