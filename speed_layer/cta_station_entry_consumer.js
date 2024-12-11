'use strict';
require('dotenv').config();
const kafka = require('kafka-node');
const axios = require('axios');

// Configuration
const HBASE_REST_URL = process.argv[2];
const KAFKA_BROKERS = process.argv[3];
const KAFKA_TOPIC = 'kjwassell_station_entries';

// Table Names
const TOTAL_RIDES_TABLE = 'kjwassell_cta_total_rides_by_day_hbase';
const RIDERSHIP_WITH_DAY_TABLE = 'kjwassell_cta_ridership_with_day_hbase';

// Kafka Consumer Setup
const client = new kafka.KafkaClient({ kafkaHost: KAFKA_BROKERS });
const consumer = new kafka.Consumer(
    client,
    [{ topic: KAFKA_TOPIC, partition: 0 }],
    { autoCommit: true, fromOffset: 'earliest' }
);

console.log(`Listening to Kafka topic: ${KAFKA_TOPIC}`);

// Get the current day key (e.g., 'M' for Monday)
function getDayKey() {
    const days = ['Su', 'M', 'T', 'W', 'Th', 'F', 'S'];
    const dayIndex = new Date().getDay();
    return days[dayIndex];
}

// Get the current date as a string
function getCurrentDate() {
    const today = new Date();
    return today.toISOString().split('T')[0];
}

// HBase Increment Helper
async function incrementCounter(tableName, rowKey, column) {
    const incrementUrl = `${HBASE_REST_URL}/${tableName}/${rowKey}/${column}`;
    try {
        await axios.post(incrementUrl, '1', {
            headers: { 'Content-Type': 'text/plain' },
        });
        console.log(`Incremented ${tableName} at ${rowKey}:${column}`);
    } catch (error) {
        console.error(`Error incrementing ${tableName} at ${rowKey}:${column}`, error);
    }
}

// HBase Put Row Helper
async function putRow(tableName, rowKey, data) {
    const putUrl = `${HBASE_REST_URL}/${tableName}/${rowKey}`;
    const cells = Object.keys(data).map((key) => ({
        column: Buffer.from(key).toString('base64'),
        $: Buffer.from(data[key]).toString('base64'),
    }));

    try {
        await axios.put(
            putUrl,
            { Row: [{ key: Buffer.from(rowKey).toString('base64'), Cell: cells }] },
            { headers: { 'Content-Type': 'application/json' } }
        );
        console.log(`Inserted/Updated row in ${tableName}: ${rowKey}`);
    } catch (error) {
        console.error(`Error inserting/updating ${tableName} at ${rowKey}`, error);
    }
}

// Process Kafka Messages
consumer.on('message', async (message) => {
    console.log('Received message:', message.value);
    try {
        const data = JSON.parse(message.value); // Parse incoming JSON message
        const stationId = data.station; // Extract station ID
        const entryNumber = parseInt(data.entry_number, 10); // Entry count
        const dayKey = getDayKey(); // Get current day key
        const currentDate = getCurrentDate(); // Get today's date
        const rowKeyTotalRides = `${stationId}_${dayKey}`; // Total rides row key
        const rowKeyRidership = `${stationId}_${currentDate}`; // Ridership with day row key

        console.log('Processing station entry:', {
            stationId,
            entryNumber,
            dayKey,
            currentDate,
            rowKeyTotalRides,
            rowKeyRidership,
        });

        // Increment total rides by day
        await incrementCounter(TOTAL_RIDES_TABLE, rowKeyTotalRides, 'data:total_rides');

        // Insert/Update ridership with day table
        const ridershipRow = {
            'data:station_id': stationId,
            'data:station_name': 'Unknown', // Replace with actual station name if available
            'data:date': currentDate,
            'data:day': dayKey,
            'data:rides': entryNumber.toString(),
        };
        await putRow(RIDERSHIP_WITH_DAY_TABLE, rowKeyRidership, ridershipRow);

        console.log(`Processed station entry: ${stationId}`);
    } catch (error) {
        console.error('Error processing message:', error);
    }
});

// Handle Consumer Errors
consumer.on('error', (error) => {
    console.error('Kafka Consumer error:', error);
});
