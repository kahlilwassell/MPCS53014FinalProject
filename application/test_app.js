'use strict';
const http = require('http');
const express = require('express');
const mustache = require('mustache');
const filesystem = require('fs');
const app = express();
const hbase = require('hbase');

// Port and HBase client configuration
const port = Number(process.argv[2]) || 3000; // Default port 3000
const hclient = hbase({ host: process.argv[3] || 'localhost', port: Number(process.argv[4]) || 9090 });

// Helper Functions
function removePrefix(text, prefix) {
    if (!text.startsWith(prefix)) {
        throw new Error("Missing prefix");
    }
    return text.substring(prefix.length);
}

function rowToMap(row) {
    let stats = {};
    row.forEach(item => {
        stats[item['column']] = Number(item['$']);
    });
    return stats;
}

function groupByStation(cells) {
    let result = {};
    cells.forEach(cell => {
        let rowKey = removePrefix(cell['key'], '');
        let column = removePrefix(cell['column'], 'data:');
        if (!result[rowKey]) result[rowKey] = {};
        result[rowKey][column] = cell['$'];
    });
    return Object.keys(result).map(key => ({ rowKey: key, ...result[key] }));
}

// Express Setup
app.use(express.static('public'));

// Endpoints
app.get('/daily-ridership', (req, res) => {
    hclient.table('kjwassell_daily_station_ridership_hbase').scan(
        { maxVersions: 1 },
        (err, cells) => {
            if (err) {
                res.status(500).send("Error fetching data: " + err.message);
                return;
            }
            let data = groupByStation(cells);
            let template = filesystem.readFileSync("templates/daily.mustache").toString();
            let html = mustache.render(template, { records: data });
            res.send(html);
        }
    );
});

app.get('/monthly-ridership', (req, res) => {
    hclient.table('kjwassell_monthly_ridership_hbase').scan(
        { maxVersions: 1 },
        (err, cells) => {
            if (err) {
                res.status(500).send("Error fetching data: " + err.message);
                return;
            }
            let data = groupByStation(cells);
            let template = filesystem.readFileSync("templates/monthly.mustache").toString();
            let html = mustache.render(template, { records: data });
            res.send(html);
        }
    );
});

// Start the application
app.listen(port, () => {
    console.log(`Web application is running at http://localhost:${port}`);
});
