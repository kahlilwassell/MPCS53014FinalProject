'use strict';

const kafka = require('kafka-node');

// Configuration
const KAFKA_BROKERS = process.argv[2];
const KAFKA_TOPIC = 'kjwassell_station_entries';

// Kafka Consumer Setup
const client = new kafka.KafkaClient({ kafkaHost: KAFKA_BROKERS });
const consumer = new kafka.Consumer(
    client,
    [{ topic: KAFKA_TOPIC, partition: 0 }],
    { autoCommit: true, fromOffset: 'earliest' }
);
console.log(`Listening to Kafka topic: ${KAFKA_TOPIC} on ${KAFKA_BROKERS}`);

consumer.on('message', (message) => {
    console.log('Received message:', message.value);
});

consumer.on('error', (err) => {
    console.error('Kafka Consumer Error:', err);
});
