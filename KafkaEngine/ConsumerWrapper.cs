namespace KafkaEngine
{
    using Confluent.Kafka;
    using System;
    using System.Threading;
    public class ConsumerWrapper<T>
    {
        private string _topicName;
        private ConsumerConfig _consumerConfig;
        private Consumer<string,T> _consumer;
        public ConsumerWrapper(ConsumerConfig config,string topicName)
        {
            this._topicName = topicName;
            this._consumerConfig = config;
            this._consumer = new Consumer<string,T>(this._consumerConfig);
            this._consumer.Subscribe(topicName);
        }
        public T readMessage(){
            var consumeResult = this._consumer.Consume();
            return consumeResult.Value;
        }
    }
}