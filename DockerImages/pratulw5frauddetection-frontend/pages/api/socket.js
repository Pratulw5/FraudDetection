// pages/api/socket.js
import { Server } from 'socket.io';
import { initKafkaConsumers, setIOInstance } from '../../lib/kafkaConsumer';

let isKafkaStarted = false;

export default async function handler(req, res) {
  if (!res.socket.server.io) {
    console.log('🔌 Setting up Socket.IO server...');
    const io = new Server(res.socket.server, {
      path: '/api/socket',
    });

    res.socket.server.io = io;
    setIOInstance(io);

    if (!isKafkaStarted) {
      await initKafkaConsumers();
      isKafkaStarted = true;
    }
  }
  res.end();
}
