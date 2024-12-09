'use strict';
require('dotenv').config();
const axios = require('axios');
const kafka = require('kafka-node');
const hbase = require('hbase');

// Configuration
const CTA_API_KEY = process.env.CTA_API_KEY || '1cac44e9a4544e79a0c2a0e1cf933925';
const API_BASE_URL = 'http://lapi.transitchicago.com/api/1.0/';
const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092';
const TRAIN_ARRIVALS_TOPIC = 'kjwassell_train_locations';

// HBase Client Configuration
const HBASE_TABLE = 'kjwassell_cta_station_view_hbase';

const url = new URL(process.env.HBASE_REST_URL);

const hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port ?? 'http' ? 80 : 443, // http or https defaults
    protocol: url.protocol.slice(0, -1), // Don't want the colon
    encoding: 'latin1',
    auth: process.env.HBASE_AUTH
});

// Kafka setup
const client = new kafka.KafkaClient({ kafkaHost: KAFKA_BROKERS });
const producer = new kafka.Producer(client);

// Function to fetch `map_id` values from HBase
async function fetchMapIdsFromHBase() {
    return new Promise((resolve, reject) => {
        const mapIds = [];
        hclient
            .table(HBASE_TABLE)
            .scan({ maxVersions: 1 }, (error, rows) => {
                if (error) {
                    console.error('Error scanning HBase table:', error);
                    reject([]);
                    return;
                }

                rows.forEach((row) => {
                    const mapIdCell = row.columnValues.find((cell) => cell.qualifier === 'map_id');
                    if (mapIdCell) {
                        const mapId = parseInt(mapIdCell.value.toString('utf8'), 10); // Decode `map_id`
                        mapIds.push(mapId);
                    }
                });

                console.log('Fetched map IDs:', mapIds);
                resolve(mapIds);
            });
    });
}

// Fetch train arrivals for multiple stations
async function fetchTrainArrivals(mapIds) {
    const url = `${API_BASE_URL}ttarrivals.aspx`;
    const params = {
        key: CTA_API_KEY,
        mapid: mapIds.join(','), // Join up to 4 `map_id`s with a comma
        outputType: 'JSON',
    };

    try {
        const response = await axios.get(url, { params });
        if (response.status === 200) {
            return response.data;
        } else {
            console.error(`Error fetching train arrivals for map IDs ${mapIds}: ${response.status} - ${response.statusText}`);
            return null;
        }
    } catch (error) {
        console.error(`Exception while fetching train arrivals for map IDs ${mapIds}:`, error);
        return null;
    }
}

// Publish data to Kafka
function publishToKafka(topic, data) {
    const payloads = [{ topic, messages: JSON.stringify(data) }];
    producer.send(payloads, (error, result) => {
        if (error) {
            console.error('Error publishing to Kafka:', error);
        } else {
            console.log(`Data published to ${topic}:`, result);
        }
    });
}

// Process train arrivals for all stations in batches of 4
async function processTrainArrivals(stations) {
    for (let i = 0; i < stations.length; i += 4) {
        const batch = stations.slice(i, i + 4); // Get a batch of up to 4 map IDs
        console.log(`Fetching train arrivals for map IDs: ${batch}`);
        const data = await fetchTrainArrivals(batch);
        if (data) {
            const relevantData = data.ctatt?.eta || [];
            publishToKafka(TRAIN_ARRIVALS_TOPIC, relevantData);
        } else {
            console.log(`No data available for map IDs: ${batch}`);
        }
    }
}

// Main loop
async function main() {
    // Fetch station map IDs
    const stations = await fetchMapIdsFromHBase();
    if (!stations.length) {
        console.error('No stations found. Retrying...');
        await new Promise((resolve) => setTimeout(resolve, 90000));
    }
    while (true) {
        try {
            // Process train arrivals
            await processTrainArrivals(stations);

            // Wait 90 seconds before next fetch
            await new Promise((resolve) => setTimeout(resolve, 90000));
        } catch (error) {
            console.error('Error in main loop:', error);
        }
    }
}

// Ensure Kafka producer is ready before starting
producer.on('ready', () => {
    console.log('Kafka Producer is connected and ready.');
    main();
});

producer.on('error', (error) => {
    console.error('Kafka Producer error:', error);
});
