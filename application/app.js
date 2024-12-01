'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()
const port = Number(process.argv[2]);
const hbase = require('hbase')
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
            console.warn("Encountered non-numeric value or unsupported encoding:", encodedValue);
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
app.get('/cta_stop_summary.html',function (req, res) {
    const station=req.query['station'];

    if (!station) {
        res.status(400).send("Please provide a station name.");
        return;
    }

 // Query HBase
    hclient.table('kjwassell_cta_avg_rides_by_day_hbase')
        .row(`40010_${day}`)
        .get((err, row) => {
            if (err) {
                console.error("Error retrieving data from HBase:", err);
                res.status(500).send("Error fetching data.");
                return;
            }

            if (!row || row.length === 0) {
                res.status(404).send("No data found for the specified station and day.");
                return;
            }

            const data = rowToMap(row);

            const template = filesystem.readFileSync("result.mustache").toString();
            const rendered = mustache.render(template, {
                station_name: data['data:station_name'],
                day,
                avg_rides: data['data:avg_rides']
            });

            res.send(rendered);
        });
});

// Start server
app.listen(port, () => {
    console.log(`App running at http://localhost:${port}`);
});