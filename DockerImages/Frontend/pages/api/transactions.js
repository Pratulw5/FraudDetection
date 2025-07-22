import { KafkaClient, Consumer } from 'kafka-node';

let messages = [];

let initialized = false;

export default function handler(req, res) {
  if (!initialized) {
    const client = new KafkaClient({ kafkaHost: 'kafka:9092' });
    const consumer = new Consumer(client, [{ topic: 'high-value-transactions' }], {
      autoCommit: true,
    });

    consumer.on('message', (msg) => {
      try {
        const parsed = JSON.parse(msg.value);
        messages.push(parsed);
        if (messages.length > 100) messages.shift();
      } catch (e) {}
    });

    initialized = true;
  }

  res.status(200).json(messages);
}
