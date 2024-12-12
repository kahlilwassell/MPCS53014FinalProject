# MPCS53014FinalProject
This was my Final Project for the UChicago course MPCS53014 Big Data Application Architecture.

# CTA Transit L Monitoring Application

## Overview

The CTA Transit L Monitoring Application is a comprehensive data-driven solution designed to provide real-time insights into Chicago Transit Authority (CTA) L train operations. This project combines historical data analysis, and real-time streaming to enhance user experience, improve transit planning, and optimize station and train management.

The application integrates multiple technologies, including Kafka, HBase, Hive, and a web front end, to deliver a rich set of features such as real-time train tracking, station metrics, and predicted crowding analysis. This application loosely follows the lambda architecture using a data lake, a batch layer, a serving layer, and a speed layer.

---

## Features

### 1. **Historical Analysis**
   - **Daily Average Ridership by Station and Day**: Displays historical average ridership data for any station and day of the week.
   - **Total Rides by Station and Day**: Tracks cumulative ridership totals and updates incrementally based on real-time station entry events.

### 2. **Real-Time Analytics**
   - **Train Location Tracking**: Visualizes current train locations and movements along selected routes using live data from the CTA Train Tracker API.
   - **Station Arrival Tracking**: Diplays current train arrival predictions for a given station using the CTA Train Tracker API.
   - **Station Entry Tracking**: Captures station entry events in real time via Kafka and updates batch views in HBase.
   - **Interactive Maps**: Displays train locations on a map for selected routes, aiding in passenger decision-making.

### 3. **Web Interface**
   - **Dropdown Selections**: Users can select a station or train line to view corresponding metrics and visualizations.
   - **Real-Time Visualizations**: Presents dynamic information about train locations and station metrics.
   - **User Input Integration**: Allows users to log station entries, which feed directly into the real-time processing pipeline.

---

## Architecture

### Data Flow
1. **Historical Data Pipeline**:
   - Data ingested into HBase and Hive for historical analysis.
   - The Primary Data sources of the application are the data sets for [CTA ridership](https://data.cityofchicago.org/Transportation/CTA-Ridership-L-Station-Entries-Daily-Totals/5neh-572f/about_data) and [CTA stops](https://data.cityofchicago.org/Transportation/CTA-System-Information-List-of-L-Stops/8pix-ypme/about_data) In addition to the real time [cta train tracker api](https://www.transitchicago.com/assets/1/6/cta_Train_Tracker_API_Developer_Guide_and_Documentation.pdf).
   - Tables include:

    **Data Lake / Batch Layer**
    - `kjwassell_cta_ridership_csv` (data lake ridership representation in Hive)
    - `kjwassell_cta_ridership_orc` (data lake ridership representation in Hive or representation for more efficient querying and view creation)
    - `kjwassell_cta_stations_csv` (data lake station representation in Hive)
    - `kjwassell_cta_stations_orc` (data lake station representation in Hive or representation for more efficient querying and view creation)
    - all of the above tables follow the sushio principle directly mirroring the data they are extracted from.
    
    **Serving Layer**
    - `kjwassell_cta_station_view_orc` (representation of station with all lines serviced and accessibility information)
    - `kjwassell_cta_ridership_with_day_orc` (representation of ridership information with additional information (day of week) included)
    - `kjwassell_cta_total_rides_by_day_orc` (representation of ridership information aggregating on day of week and summing up rides and number of days for that day of the week)
    - `kjwassell_cta_station_view_hbase` (representation of station with all lines serviced and accessibility information to serve web application)
    - `kjwassell_cta_ridership_with_day_hbase` (representation of ridership information with additional information (day of week) included)
    - `kjwassell_cta_total_rides_by_day_hbase` (representation of ridership information aggregating on day of week and summing up rides and number of days for that day of the week to server web application)
    - `kjwassell_station_name_to_station_id_hbase` (hbase mapping of station name to station id to populate dropdown in application and facilitate querying)
    - each of these are more contrived views to glean more meaningful insights from.


2. **Real-Time Pipeline**:
   - **Kafka**:
     - `kjwassell_station_entries`: Logs real-time station entries from user inputs in the web application.
   - **Spark/Scala Streaming**:
     - Processes station entry events and updates HBase tables dynamically.
   - **HBase**:
     - Stores batch and real-time views of station entry data. incrementing the data for our historical tables.

3. **Web Front End**:
   - **Javascript, CSS, HTML**
   - Interactive forms for station and train line selection.
   - Visualization of station metrics (average entries for the given day of the week and selected station and predicted crowding).
   - Form to feed real time entries into the application by station.

---

## Technology Stack

### Core Components
- **Hive**: Query and manage historical data.
- **HBase**: Batch and real-time views of ridership and train data.
- **Kafka**: Real-time message broker for and station entry streams.
- **Spark Streaming**: Processes Kafka streams to update HBase and provide real-time analytics.
- **Python/Scala**: For Kafka producers and Spark jobs.
- **JavaScript, HTML, CSS**: Front-end implementation.

### APIs and Libraries
- **CTA Train Tracker API**: Source of real-time train location data.
- **Moment.js**: Handles date and time formatting in the front end.
- **Jackson & Spark Kafka**: Manages JSON data deserialization in Scala.
- **HBase Client**: Interacts with HBase for reading and writing.

---

## Installation and Setup

### Prerequisites
1. **Environment**:
   - A distributed cluster with Kafka, HBase, Hive, and Spark installed.
   - Node.js for running the web application.
   - Maven for building Scala jobs.

2. **API Key**:
   - Obtain a CTA Train Tracker API key and update the `.env` file.

### Steps
1. **Clone the Repository**:
   `git clone https://github.com/yourusername/cta-transit-optimization.git`
   `cd cta-transit-optimization`

2. **Install Dependencies**
  - Node.js: `npm install`
  - Maven (for Scala Spark jobs): `mvn clean install`

3. **Set Up Kafka Topics**: bash kafka-topics.sh --create --topic train-locations --bootstrap-server $KAFKA_BROKERS kafka-topics.sh --create --topic kjwassell_station_entries --bootstrap-server $KAFKA_BROKERS

4. **Run Spark Streaming Jobs**:
spark-submit --class StreamStationEntries \
    --master yarn \
    --deploy-mode cluster \
    --jars /path/to/hbase-client.jar,/path/to/spark-streaming-kafka.jar \
    target/stream_kafka_station_entries-1.0-SNAPSHOT.jar $KAFKA_BROKERS

5. **Start the Web Application**: `node app.js 3000`

### Usage
The github page for this repo can be found [here](https://github.com/kahlilwassell/MPCS53014FinalProject/blob/main/README.md). Cloning this repo will give you access to all of the code I used to create and run this application.
Here is a list of the locations of the mot important components
1) The front end Application code can be found at `~/kjwassell/final_app`
2) The jar for the Kafka Consumer can be found at  `~/kjwassell/KafkaToHBase/target`
3) I have manually cloned the rest of the repo here as well at `~/kjwassell/MPCS53014FinalProject`

**Steps for Running the Application**
1) ssh into the cluster `ssh -i /Users/kahlilwassell/.ssh/id_MPCS53014_rsa sshuser@hbase-mpcs53014-2024-ssh.azurehdinsight.net`
  - All of the relevant fundmental data for this application can be found in `~/kjwassell/MPCS53014FinalProject/raw_data`
2) navigate to `~/kjwassell/final_app`
3) start up application `node app.js 3001 https://hbase-mpcs53014-2024.azurehdinsight.net/hbaserest $KAFKABROKERS`
4) navigate to `~/kjwassell/KafkaToHBase/target`
5) start up the kafka consumer to process live updates `spark-submit --driver-java-options "-Dlog4j.configuration=file:///home/sshuser/ss.log4j.properties" --class StreamStationEntries uber-KafkaToHBase-1.0-SNAPSHOT.jar $KAFKABROKERS`
6) Set up socks proxy to proxy application `https://edstem.org/us/courses/68329/discussion/5855877`
![Screenshot 2024-12-12 at 12 09 58 AM](https://github.com/user-attachments/assets/bf3b4dc0-0bb6-44da-8cc6-239b54a0f2e0)
7) run the command for tunneling locally `ssh -i /Users/kahlilwassell/.ssh/id_MPCS53014_rsa -C2qTnNf -D 9876 sshuser@hbase-mpcs53014-2024-ssh.azurehdinsight.net`
8) navigate to url `http://10.0.0.38:3001`
9) Select a station to view the set of incoming trains and the average entries for the current day of the week.
10) Navigate to `http://10.0.0.38:3001/submit_entry.html` and select the station and the number of passengers entering.
**Kafka Monitoring:**
    `kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKERS --topic train-locations`
    
### Demos

https://github.com/user-attachments/assets/47f21285-1627-427c-9f71-59dddace0b9f



https://github.com/user-attachments/assets/6a142758-f94c-4b47-97de-97ddfb2eb6fd

![Screenshot 2024-12-12 at 1 10 17 AM](https://github.com/user-attachments/assets/78bbed2f-67f1-4f90-8458-1e31aeff14a4)


**Future Enhancements**
1. Enhanced Predictive Models:
  - Incorporate weather and event data for better crowding predictions in addition to day/month machine learning.
2. Real-Time Alerts:
  - Notify users of train delays or heavy crowding.
3. Expanded Visualization:
  - Heatmaps for crowding across the entire transit network, and a real time view of train locations.

**Last Minute Notes**

I figured out there was a small bug in my speed layer at the last moment. This was in updating one of the tables that I have for the kafka consumer where it will overwrite the number of rides accidentally for one of the core tables. I would fix this if there was time but there is not.

**Contributors**
  - Kahlil Wassell (Project Lead and Developer)

**Acknowledgments**
Special thanks to the University of Chicago MPCS53014 course and Professor Spertus along with the rest of the course staff for inspiring this project and providing foundational knowledge on big data systems and architecture.
