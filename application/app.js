'use strict';
const http = require('http');
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config();
const hbase = require('hbase');
const moment = require('moment');
const axios = require('axios');

const port = Number(process.argv[2]);
const url = new URL(process.env.HBASE_REST_URL);

var hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port ?? 'http' ? 80 : 443, // http or https defaults
    protocol: url.protocol.slice(0, -1), // Don't want the colon
    encoding: 'latin1',
    auth: process.env.HBASE_AUTH
});

// Function to decode binary-encoded numeric values
function decodeValue(encodedValue) {
    const buffer = Buffer.from(encodedValue, 'latin1'); // Convert the value to a Buffer

    // Handle boolean values (single byte)
    if (buffer.length === 1) {
        const booleanValue = buffer[0];
        if (booleanValue === 0x00) {
            return false; // Decode as `false`
        } else if (booleanValue === 0x01) {
            return true; // Decode as `true`
        }
    }

    // Handle numeric values
    switch (buffer.length) {
        case 4: // 4-byte buffer (likely a float)
            return buffer.readFloatBE();
        case 8: // 8-byte buffer (likely a BigInt64 or Double)
            try {
                // Try to decode as a 64-bit BigInt (integer)
                return Number(buffer.readBigInt64BE());
            } catch (err) {
                // If decoding as BigInt fails, decode as Double (floating point)
                return buffer.readDoubleBE();
            }
        default:
            console.warn("Encountered unsupported encoding or non-numeric value:", encodedValue);
            return encodedValue; // Return the original value if it's not recognized
    }
}


// Function to map a row of HBase data to a more readable format
function rowToMap(row) {
    const data = {};
    row.forEach(function (item) {
        data[item['column']] = decodeValue(item['$']);
    });
    return data;
}

// Get the current day of the week (e.g., "M", "T", ...)
const day = moment().format('dd');

// application code
app.use(express.static('public'));
// Endpoint to get stop summary including arrivals
app.get('/cta_stop_summary.html', async function (req, res) {
    const station = req.query['station'];

    if (!station) {
        res.status(400).send("Please provide a station name.");
        return;
    }

    try {
        // Query HBase for daily average rides
        const avgRideData = await new Promise((resolve, reject) => {
            hclient.table('kjwassell_cta_avg_rides_by_day_hbase')
                .row(`${station}_${moment().format('dd')}`)
                .get((err, row) => {
                    if (err || !row) return reject("No data found for daily average rides.");
                    resolve(rowToMap(row));
                });
        });

        // Query HBase for station view data
        const stationViewData = await new Promise((resolve, reject) => {
            hclient.table('kjwassell_cta_station_view_hbase')
                .row(station)
                .get((err, row) => {
                    if (err || !row) return reject("No station view data found.");
                    resolve(rowToMap(row));
                });
        });

        // Fetch train arrivals from the API
        const mapId = stationViewData['data:map_id'];
        const trainArrivals = await axios.get(`${process.env.API_BASE_URL}${process.env.ARRIVALS_ENDPOINT}`, {
            params: {
                key: process.env.CTA_API_KEY,
                mapid: mapId,
                outputType: 'JSON',
                max: 10,
            },
        });

        const arrivalsData = trainArrivals.data.ctatt?.eta || [];

        // Render the response using Mustache
        const template = filesystem.readFileSync("result.mustache").toString();

        function formatDateTime(isoString) {
            return moment(isoString).format('dddd, MMMM Do YYYY, h:mm:ss A');
        }

        const rendered = mustache.render(template, {
            station_name: avgRideData['data:station_name'],
            day: moment().format('dddd'),
            avg_rides: Math.round(avgRideData['data:avg_rides']),
            ada: stationViewData['data:ada'] === 'true' ? "Yes" : "No",
            lines_serviced: [
                stationViewData['data:red'] ? "Red" : null,
                stationViewData['data:blue'] ? "Blue" : null,
                stationViewData['data:g'] ? "Green" : null,
                stationViewData['data:brn'] ? "Brown" : null,
                stationViewData['data:p'] ? "Purple" : null,
                stationViewData['data:pexp'] ? "Purple Express" : null,
                stationViewData['data:y'] ? "Yellow" : null,
                stationViewData['data:pnk'] ? "Pink" : null,
                stationViewData['data:o'] ? "Orange" : null,
            ].filter(Boolean).join(", "),
            arrivals: arrivalsData.map(arrival => ({
                route: arrival.rt,
                destination: arrival.destNm,
                arrival_time: formatDateTime(arrival.arrT),
                is_approaching: arrival.isApp === "1" ? "Yes" : "No",
            })),
        });

        res.send(rendered);
    } catch (error) {
        console.error(error);
        res.status(500).send("Error fetching data.");
    }
});

// Start server
app.listen(port, () => {
    console.log(`App running at http://localhost:${port}`);
});
