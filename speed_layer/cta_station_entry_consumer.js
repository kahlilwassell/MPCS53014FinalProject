'use strict';
require('dotenv').config();
const kafka = require('kafka-node');
const axios = require('axios');

// Configuration
const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';
const KAFKA_TOPIC = 'kjwassell_station_entries';
const HBASE_REST_URL = process.env.HBASE_REST_URL || 'http://hbase-mpcs53014-2024.azurehdinsight.net/hbaserest';
const HBASE_TABLE = 'kjwassell_cta_total_rides_by_day_hbase';

// Initialize Kafka Consumer
const client = new kafka.KafkaClient({ kafkaHost: KAFKA_BROKERS });
const consumer = new kafka.Consumer(
    client,
    [{ topic: KAFKA_TOPIC, partition: 0 }],
    { autoCommit: true }
);

console.log(`Listening to Kafka topic: ${KAFKA_TOPIC}`);

// Function to get the current day key (e.g., 'M' for Monday)
function getDayKey() {
    const days = ['Su', 'M', 'T', 'W', 'Th', 'F', 'S'];
    const dayIndex = new Date().getDay();
    return days[dayIndex];
}

// Process Kafka Messages
consumer.on('message', async (message) => {
    try {
        const data = JSON.parse(message.value); // Parse incoming JSON message
        const stationId = data.station; // Extract station ID
        const dayKey = getDayKey(); // Get current day key
        const rowKey = `${stationId}_${dayKey}`; // Create HBase row key

        console.log(`Processing station entry: ${rowKey}`);

        // Increment HBase counter
        const incrementUrl = `${HBASE_REST_URL}/${HBASE_TABLE}/${rowKey}/data:total_rides`;
        await axios.post(incrementUrl, '1', {
            headers: { 'Content-Type': 'text/plain' },
        });

        console.log(`Successfully incremented counter for row: ${rowKey}`);
    } catch (error) {
        console.error('Error processing message:', error);
    }
});

// Handle Consumer Errors
consumer.on('error', (error) => {
    console.error('Kafka Consumer error:', error);
});
