import { Kafka } from 'kafkajs';
import { saveMetricToDB } from './saveMetric.js';
import { savePredictionToDB } from './savePrediction.js';
import { createTables } from './initTables.js'; // or wherever it's defined
// initTables.js







const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['kafka:9092'],
});

const consumerPredictions = kafka.consumer({ groupId: 'predictions-group' });
const consumerMetrics = kafka.consumer({ groupId: 'metrics-group' });

let ioInstance = null;

export const setIOInstance = (io) => {
  ioInstance = io;
};

export const initKafkaConsumers = async () => {
  await createTables();
  await consumerPredictions.connect();
  await consumerMetrics.connect();

  await consumerPredictions.subscribe({ topic: 'Predictions', fromBeginning: false });
  await consumerMetrics.subscribe({ topic: 'Metrics', fromBeginning: false });

  // Run both consumers concurrently without awaiting
  const predictionConsumer = consumerPredictions.run({
    
    eachMessage: async ({ message }) => {
       console.log('Received message:', message.value.toString());  
      if (ioInstance) {
        console.log("intoo the preditiocns");
        const parsed = JSON.parse(message.value.toString());
        ioInstance.emit('predictionUpdate', parsed);
        try {
        await savePredictionToDB(parsed);
        } catch (err) {
          console.error('Failed to save metric to DB Prediction:', err);
        }
      }
      // Save to DB
      
    },
  });

  const metricsConsumer = consumerMetrics.run({
    eachMessage: async ({ message }) => {
      console.log("intoo the mterics");
      console.log('Received message:', message.value.toString());  // Log raw message
      // Emit to frontend
      if (ioInstance) {
        
        const parsed = JSON.parse(message.value.toString());
        console.log('Parsed:', parsed);  // Log parsed message
        ioInstance.emit('metricsUpdate', parsed);
        try {
        await saveMetricToDB(parsed);
        } catch (err) {
          console.error('Failed to save metric to DB Metrics:', err);
        }
      }
      // Save to DB
      
    },
  });

  // Wait for both consumers to run concurrently
  await Promise.all([predictionConsumer, metricsConsumer]);
};
