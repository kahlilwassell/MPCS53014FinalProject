'use strict';

require('dotenv').config();
const kafka = require('kafka-node');
const hclient = require('hclient');

// Configuration
const HBASE_REST_URL='https://hclient-mpcs53014-2024.azurehdinsight.net/hbaserest'
const HBASE_AUTH='admin:@a*mJuBS&jA@A8f'
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

const url = new URL(HBASE_REST_URL);

var hclient = hclient({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port ?? 'http' ? 80 : 443, // http or https defaults
    protocol: url.protocol.slice(0, -1), // Don't want the colon
    encoding: 'latin1',
    auth: HBASE_AUTH
});

async function getHBaseValue(table, rowKey, column) {
    return new Promise((resolve, reject) => {
        hclient
            .table(table)
            .row(rowKey)
            .get(column, (err, cells) => {
                if (err) {
                    console.error(`Error reading ${table}:${rowKey}:${column}`, err);
                    reject(err);
                } else if (cells && cells.length > 0) {
                    resolve(parseInt(cells[0].$ || '0', 10)); // Extract value or default to 0
                } else {
                    resolve(0); // Default to 0 if no value found
                }
            });
    });
}

async function putHBaseValue(table, rowKey, data) {
    return new Promise((resolve, reject) => {
        hclient
            .table(table)
            .row(rowKey)
            .put(data, (err, success) => {
                if (err) {
                    console.error(`Error writing to ${table}:${rowKey}`, err);
                    reject(err);
                } else {
                    console.log(`Successfully wrote to ${table}:${rowKey}`);
                    resolve(success);
                }
            });
    });
}

async function incrementHBaseCounterManually(table, rowKey, column, incrementValue) {
    try {
        const currentValue = await getHBaseValue(table, rowKey, column);
        const newValue = currentValue + incrementValue;
        await putHBaseValue(table, rowKey, { [column]: newValue.toString() });
        console.log(`Incremented ${table}:${rowKey}:${column} by ${incrementValue}, new value: ${newValue}`);
    } catch (error) {
        console.error(`Error incrementing counter manually for ${table}:${rowKey}:${column}`, error);
    }
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
        await incrementHBaseCounterManually(TOTAL_RIDES_TABLE, rowKeyTotalRides, 'data:total_rides', entryNumber);

        // Insert/Update ridership with day table
        const ridershipRow = {
            'data:station_id': stationId,
            'data:station_name': 'Unknown', // Replace with actual station name if available
            'data:date': currentDate,
            'data:day': dayKey,
            'data:rides': entryNumber.toString(),
        };
        await putHBaseValue(RIDERSHIP_WITH_DAY_TABLE, rowKeyRidership, ridershipRow);

        console.log(`Processed station entry for ${stationId}`);
    } catch (error) {
        console.error('Error processing Kafka message:', error);
    }
});

// Kafka Consumer Error Handling
consumer.on('error', (error) => {
    console.error('Kafka Consumer error:', error);
});
