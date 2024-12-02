'use strict';
const http = require('http');
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config();
const port = Number(process.argv[2]);
const hbase = require('hbase');
const moment = require('moment');

const url = new URL(process.argv[3]);

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

app.get('/cta_stop_summary.html', function (req, res) {
    const station = req.query['station'];

    if (!station) {
        res.status(400).send("Please provide a station name.");
        return;
    }

    // Query HBase for daily average rides
    hclient.table('kjwassell_cta_avg_rides_by_day_hbase')
        .row(`${station}_${day}`)
        .get((err, row) => {
            if (err) {
                console.error("Error retrieving data from HBase (avg rides):", err);
                res.status(500).send("Error fetching data.");
                return;
            }

            if (!row || row.length === 0) {
                res.status(404).send("No data found for the specified station and day.");
                return;
            }

            const avgRideData = rowToMap(row);

            // Query HBase for station view data
            hclient.table('kjwassell_cta_station_view_hbase')
                .row(`${station}`)
                .get((err, stationRow) => {
                    if (err) {
                        console.error("Error retrieving data from HBase (station view):", err);
                        res.status(500).send("Error fetching data.");
                        return;
                    }

                    if (!stationRow || stationRow.length === 0) {
                        res.status(404).send("No station view data found for the specified station.");
                        return;
                    }

                    const stationViewData = rowToMap(stationRow);

                    const template = filesystem.readFileSync("result.mustache").toString();
                    const rendered = mustache.render(template, {
                        station_name: avgRideData['data:station_name'],
                        day,
                        avg_rides: Math.round(avgRideData['data:avg_rides']),
                        ada: stationViewData['data:ada'] ? "Yes" : "No",
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
                        ].filter(line => line).join(", ") // Filter out nulls and join the lines
                    });
                    res.send(rendered);
                });
        });
});

// Start server
app.listen(port, () => {
    console.log(`App running at http://localhost:${port}`);
});
