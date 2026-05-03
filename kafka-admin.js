import { kafkaClient } from './kafka-client.js';

async function setup() {
  const admin = kafkaClient.admin();

  try {
    console.log(`Kafka Admin Connecting...`);
    await admin.connect();
    console.log(`Kafka Admin Connected`);

    console.log('Creating topics...');
    const result = await admin.createTopics({
      topics: [{ topic: 'location-updates', numPartitions: 2 }],
    });

    if (result) {
      console.log('Topic "location-updates" created successfully');
    } else {
      console.log('Topic "location-updates" already exists');
    }
  } catch (error) {
    console.error('Error in Kafka Admin:', error);
  } finally {
    await admin.disconnect();
    console.log('Kafka Admin Disconnected');
  }
}

setup();
