'use strict';
const http = require('http');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const port = Number(process.argv[2]);
const hbase = require('hbase');

// Configure HBase client
const hclient = hbase({ host: process.argv[3], port: Number(process.argv[4]) });

// Helper function to transform HBase rows into a JSON object
function rowToMap(row) {
    let data = {};
    row.forEach(item => {
        data[item.column.replace('data:', '')] = item.$;
    });
    return data;
}

// Route: Average Rides by Day
app.get('/avg_rides', (req, res) => {
    const stationId = req.query['station_id'];
    hclient.table('kjwassell_cta_avg_rides_by_day_hbase').scan(
        {
            filter: { type: "PrefixFilter", value: stationId },
            maxVersions: 1
        },
        (err, rows) => {
            if (err) {
                res.status(500).send("Error fetching data");
                return;
            }
            const rides = rows.map(row => rowToMap(row));
            const template = filesystem.readFileSync('result.mustache').toString();
            const html = mustache.render(template, { rides });
            res.send(html);
        }
    );
});

// Route: Daily Ridership
app.get('/daily_ridership', (req, res) => {
    const stationId = req.query['station_id'];
    hclient.table('kjwassell_cta_ridership_with_day_hbase').scan(
        {
            filter: { type: "PrefixFilter", value: stationId },
            maxVersions: 1
        },
        (err, rows) => {
            if (err) {
                res.status(500).send("Error fetching data");
                return;
            }
            const ridership = rows.map(row => rowToMap(row));
            const template = filesystem.readFileSync('result.mustache').toString();
            const html = mustache.render(template, { ridership });
            res.send(html);
        }
    );
});

// Route: Station Info
app.get('/station_info', (req, res) => {
    const mapId = req.query['map_id'];
    hclient.table('kjwassell_cta_station_view_hbase').scan(
        {
            filter: { type: "PrefixFilter", value: mapId },
            maxVersions: 1
        },
        (err, rows) => {
            if (err) {
                res.status(500).send("Error fetching data");
                return;
            }
            const stations = rows.map(row => rowToMap(row));
            const template = filesystem.readFileSync('result.mustache').toString();
            const html = mustache.render(template, { stations });
            res.send(html);
        }
    );
});

// Start server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
