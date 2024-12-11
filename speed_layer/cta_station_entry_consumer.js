'use strict';

require('dotenv').config();
const kafka = require('kafka-node');
const HBase = require('hbase');

// Configuration
const HBASE_HOST = 'hbase-mpcs53014-2024.azurehdinsight.net';
const HBASE_PORT = 443; // Azure HBase REST typically runs on HTTPS port
const HBASE_USER = 'admin';
const HBASE_PASSWORD = '@a*mJuBS&jA@A8f';
const KAFKA_BROKERS = process.argv[2];
const KAFKA_TOPIC = 'kjwassell_station_entries';

// Table Names
const TOTAL_RIDES_TABLE = 'kjwassell_cta_total_rides_by_day_hbase';
const RIDERSHIP_WITH_DAY_TABLE = 'kjwassell_cta_ridership_with_day_hbase';

// Kafka Consumer Setup
const client = new kafka.KafkaClient({ kafkaHost: KAFKA_BROKERS });
const consumer = new kafka.Consumer(
    client,
    [{ topic: KAFKA_TOPIC, partition: 0 }],
    { autoCommit: true, fromOffset: 'latest' }
);

console.log(`Listening to Kafka topic: ${KAFKA_TOPIC}`);

// HBase Connection
const hbase = HBase({
    host: HBASE_HOST,
    port: HBASE_PORT,
    protocol: 'https', // Use HTTPS for secure connection
    headers: {
        authorization: 'Basic ' + Buffer.from(`${HBASE_USER}:${HBASE_PASSWORD}`).toString('base64'),
    },
});

function incrementHBaseCounter(table, rowKey, column) {
    return new Promise((resolve, reject) => {
        hbase
            .table(table)
            .row(rowKey)
            .increment(column, 1, (err, success) => {
                if (err) {
                    console.error(`Error incrementing counter ${table}:${rowKey}:${column}`, err);
                    reject(err);
                } else {
                    console.log(`Successfully incremented ${table}:${rowKey}:${column}`);
                    resolve(success);
                }
            });
    });
}

function putHBaseRow(table, rowKey, data) {
    return new Promise((resolve, reject) => {
        hbase
            .table(table)
            .row(rowKey)
            .put(data, (err, success) => {
                if (err) {
                    console.error(`Error putting row into ${table}:${rowKey}`, err);
                    reject(err);
                } else {
                    console.log(`Successfully put row into ${table}:${rowKey}`);
                    resolve(success);
                }
            });
    });
}

// Helper Functions
function getDayKey() {
    const days = ['Su', 'M', 'T', 'W', 'Th', 'F', 'S'];
    const dayIndex = new Date().getDay();
    return days[dayIndex];
}

function getCurrentDate() {
    const today = new Date();
    return today.toISOString().split('T')[0];
}

// Kafka Message Processing
consumer.on('message', async (message) => {
    console.log('Received message:', message.value);
    try {
        const data = JSON.parse(message.value);
        const stationId = data.station;
        const entryNumber = parseInt(data.entry_number, 10);
        const dayKey = getDayKey();
        const currentDate = getCurrentDate();
        const rowKeyTotalRides = `${stationId}_${dayKey}`;
        const rowKeyRidership = `${stationId}_${currentDate}`;

        console.log('Processing station entry:', {
            stationId,
            entryNumber,
            dayKey,
            currentDate,
            rowKeyTotalRides,
            rowKeyRidership,
        });

        // Increment total rides by day
        await incrementHBaseCounter(TOTAL_RIDES_TABLE, rowKeyTotalRides, 'data:total_rides');

        // Insert/Update ridership with day table
        const ridershipRow = {
            'data:station_id': stationId,
            'data:station_name': 'Unknown', // Replace with actual station name if available
            'data:date': currentDate,
            'data:day': dayKey,
            'data:rides': entryNumber.toString(),
        };
        await putHBaseRow(RIDERSHIP_WITH_DAY_TABLE, rowKeyRidership, ridershipRow);

        console.log(`Processed station entry for ${stationId}`);
    } catch (error) {
        console.error('Error processing Kafka message:', error);
    }
});

// Kafka Consumer Error Handling
consumer.on('error', (error) => {
    console.error('Kafka Consumer error:', error);
});
