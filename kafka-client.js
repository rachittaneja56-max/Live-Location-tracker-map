import 'dotenv/config';
import { Kafka } from 'kafkajs';

const brokers = process.env.KAFKA_BROKERS 
  ? process.env.KAFKA_BROKERS.split(',') 
  : ['localhost:9092'];

const sasl = process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD
  ? {
      mechanism: 'plain',
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
    }
  : undefined;

const ssl = !!sasl;

export const kafkaClient = new Kafka({
  clientId: 'location-app',
  brokers,
  ssl,
  sasl,
});
