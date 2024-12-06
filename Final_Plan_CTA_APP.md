# **Final Plan to Complete CTA Transit Optimization App**

## **Overview**
This plan focuses on implementing and finalizing the following features by December 9th:
1. **Real-Time Train Location Tracking** using Kafka and Spark Streaming.
2. **Crowding Prediction** using a lightweight ML model.
3. **Front-End Visualization** of real-time train data and crowding levels.
4. Comprehensive integration and testing.

## **Timeline**
### **Day 1 (December 5th): Real-Time Train Location Pipeline**
- **Goal**: Set up a pipeline to fetch and process real-time train location data.
- **Tasks**:
  1. **Kafka Setup**:
     - Create a Kafka topic (`train-locations`) to hold train location updates.
  2. **Train Location Producer**:
     - Adjust the producer script to fetch data from the CTA API.
     - Publish train locations to Kafka.
  3. **Spark Streaming Job**:
     - Write a Spark Streaming job to consume train location data.
     - Process and filter the data (e.g., specific lines or stations).
     - Publish the processed data to another Kafka topic (`processed-locations`).
  4. **Testing**:
     - Simulate real-time data flow.
     - Ensure processed data is published to the correct Kafka topic.

---

### **Day 2 (December 6th): Crowding Prediction**
- **Goal**: Implement a machine learning model to predict station crowding levels.
- **Tasks**:
  1. **Model Training**:
     - Train a simple ML model (e.g., linear regression or decision tree) using historical ridership data.
     - Features: Time of day, day of the week, weather, past ridership.
     - Target: Crowding level (low, medium, high).
  2. **Spark Streaming Integration**:
     - Use the trained model in the Spark Streaming job.
     - Predict crowding levels for each station in the processed train location stream.
     - Publish predictions to a new Kafka topic (`crowd-predictions`).
  3. **Store Predictions in HBase**:
     - Create an HBase table (`predicted_crowding`).
     - Store predictions with the schema:
       - Row key: `station_id` + timestamp.
       - Columns: `predicted_crowding`, `timestamp`.

---

### **Day 3 (December 7th): Front-End Visualization**
- **Goal**: Build a dashboard to display real-time train data and crowding predictions.
- **Tasks**:
  1. **Real-Time Train Data**:
     - Fetch train location data from the `processed-locations` Kafka topic.
     - Display train locations as moving icons on a map (e.g., Google Maps or Leaflet.js).
  2. **Crowding Visualization**:
     - Represent crowding levels using color-coded circles around stations:
       - Green: Low crowding.
       - Yellow: Medium crowding.
       - Red: High crowding.
     - Pull crowding predictions from the `predicted_crowding` HBase table.
  3. **Combine Train and Crowding Data**:
     - Overlay train positions and crowding levels on the same map.
     - Include filters to view specific lines or stations.

---

### **Day 4 (December 8th): Integration and Testing**
- **Goal**: Ensure all components work seamlessly together.
- **Tasks**:
  1. **End-to-End Testing**:
     - Verify the data flow from the CTA API to Kafka, through Spark, and into the front-end.
  2. **Realistic Simulation**:
     - Test the app under realistic conditions (e.g., multiple users or data inflow).
  3. **Error Handling and Performance**:
     - Add robust error handling for API limits or Kafka failures.
     - Optimize performance to reduce latency.

---

### **Day 5 (December 9th): Final Enhancements**
- **Goal**: Polish the app and prepare for submission.
- **Tasks**:
  1. **User Experience Improvements**:
     - Add tooltips and explanations for real-time train updates and crowding levels.
     - Ensure the UI is intuitive and responsive.
  2. **Deployment**:
     - Package the app using Docker or deploy to a cloud platform.
     - Verify deployment stability.
  3. **Record a Demo**:
     - Create a short video showcasing the app's features.

---

## **Key Milestones**
1. **Real-Time Pipeline**:
   - Train location updates published to Kafka and displayed in the front-end.
2. **Crowding Predictions**:
   - Predictive analytics integrated into Spark and visualized in the app.
3. **Front-End Dashboard**:
   - Fully functional and user-friendly dashboard showing live and predictive data.

## **Additional Notes**
- **Tools**: 
  - Kafka, Spark, HBase, Leaflet.js/Google Maps for visualization, lightweight ML model (e.g., Scikit-learn or TensorFlow.js).
- **Focus Areas**:
  - Minimize latency in the real-time pipeline.
  - Ensure the front-end is intuitive and accessible.

---

Good luck completing your project!
