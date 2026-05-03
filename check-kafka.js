import { kafkaClient } from './kafka-client.js';

async function check() {
  const admin = kafkaClient.admin();
  try {
    console.log('Testing Kafka connection...');
    await admin.connect();
    console.log('SUCCESS: Connected to Kafka');
    const topics = await admin.listTopics();
    console.log('Topics found:', topics);
  } catch (err) {
    console.error('FAILURE: Could not connect to Kafka:', err.message);
  } finally {
    await admin.disconnect();
  }
}

check();
