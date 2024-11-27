# CTA Transit Optimization and Crowding Prediction: Adjusted Plan

## **Adjusted Steps and Timeline**

### **Step 1: Finalize Historical and Serving Layers (Already Completed)**
- Ensure the HBase tables are properly populated and queried.
- Run test queries to confirm all data is accessible, especially for use in visualizations.
- Resolve any remaining errors.

---

### **Step 2: Real-Time Pipeline Development**  
**Timeline:** Nov 24–Dec 1  

#### **Set Up Kafka**
- **Simulate Streaming Data**: Use Kafka to simulate data from the CTA Train Tracker API.  
- **Define Kafka Topics**:
  - `train_positions`
  - `train_crowding`
- **Producers**:
  - Implement Python/Node.js producers to simulate API data ingestion.

#### **Develop Spark Streaming Pipeline**
- **Build Spark Application**:
  - Consume data from Kafka topics.
  - Enrich real-time data with historical data from Hive/HBase (e.g., join train positions with ridership trends).
  - Write processed results back to HBase tables or a fast-access store like Redis for immediate querying.
- **Alerts**:
  - Implement logic to trigger alerts for overcrowded stations.

---

### **Step 3: Web Application for Visualization**  
**Timeline:** Dec 2–Dec 7  

#### **Web App Setup**
- Use **Express.js** for the backend (based on the provided architecture).
- Fetch data from HBase using the **hbase Node.js library**:
  - Query ridership trends (e.g., monthly or daily crowding levels).
  - Display real-time crowding predictions for stations/trains.
  - Suggest alternate routes for congested areas.

#### **Frontend Development**
- Use **Mustache templates** for simplicity or **React.js** for dynamic updates.
- **Key Views**:
  - Real-time train map with crowding levels.
  - Historical trends dashboard (e.g., busiest stations, monthly ridership).
  - User-configurable alerts (e.g., set a favorite station to receive updates).

#### **Integration with Real-Time Data**
- Display live data from the Kafka/Spark pipeline (stored in HBase or Redis).
- Add visual alerts for high-crowding scenarios.

---

### **Step 4: Testing and Final Adjustments**  
**Timeline:** Dec 8–Dec 9  

#### **Simulate Scenarios**
- Test with simulated high-traffic scenarios (e.g., rush hour data).
- Validate the accuracy of predictions and real-time updates.

#### **Optimize Performance**
- Tune Kafka partitions, Spark batch interval, and HBase queries.
- Test API latency and error handling.

#### **Prepare Deliverables**
- Document key features and how to use the app.
- Record a demo video showing the working application, highlighting its features.

---

## **Key Dashboard Metrics for Your App**

### **Real-Time Crowding Levels**
- Train crowding at specific stations.
- Heatmaps for station usage.

### **Historical Trends**
- Top 5 busiest stations by month/day.
- Trends by hour of the day (e.g., morning/evening rush).

### **Route Optimization Suggestions**
- Real-time alerts for alternate routes.

### **User-Specific Alerts**
- Notifications for favorite routes/stations.
- Overcrowding alerts.

---

## **Technology Stack Recap**

### **Data Ingestion and Processing**
- **Kafka**: Stream real-time CTA API data.
- **Spark Streaming**: Process real-time data.
- **HDFS, Hive, HBase**: Store and query historical and real-time data.

### **Application Layer**
- **Express.js**: Backend for web application.
- **Mustache/React.js**: Frontend visualization.
- **HBase Node.js library**: Query data.

---
