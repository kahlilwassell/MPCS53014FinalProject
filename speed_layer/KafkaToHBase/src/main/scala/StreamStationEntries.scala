import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

case class StationEntry(station: String, station_name: String, entry_number: Int, date: String)

object StreamStationEntries {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  val hbaseConf: Configuration = HBaseConfiguration.create()
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val totalRidesTable: Table = hbaseConnection.getTable(TableName.valueOf("kjwassell_cta_total_rides_by_day_hbase"))
  val ridershipTable: Table = hbaseConnection.getTable(TableName.valueOf("kjwassell_cta_ridership_with_day_hbase"))

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        """
          |Usage: StreamStationEntries <brokers>
          |  <brokers> is a list of one or more Kafka brokers
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    val sparkConf = new SparkConf().setAppName("StreamStationEntries")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "cta_station_entries_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicsSet = Set("kjwassell_station_entries")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    val serializedRecords = stream.map(_.value)
    val entries = serializedRecords.map(record => {
      try {
        mapper.readValue(record, classOf[StationEntry])
      } catch {
        case e: Exception =>
          println(s"Error deserializing record: $record")
          println(e.getMessage)
          null
      }
    }).filter(_ != null)

    entries.foreachRDD { rdd =>
      rdd.foreach { entry =>
        try {
          val dayKey = java.time.ZonedDateTime.now(java.time.ZoneId.of("America/Chicago"))
            .getDayOfWeek match {
            case java.time.DayOfWeek.SUNDAY    => "Su"
            case java.time.DayOfWeek.MONDAY    => "M"
            case java.time.DayOfWeek.TUESDAY   => "T"
            case java.time.DayOfWeek.WEDNESDAY => "W"
            case java.time.DayOfWeek.THURSDAY  => "Th"
            case java.time.DayOfWeek.FRIDAY    => "F"
            case java.time.DayOfWeek.SATURDAY  => "S"
          }

          val currentDate = java.time.LocalDate.now().toString

          // Row keys for HBase
          val totalRidesRowKey = s"${entry.station}_$dayKey"
          val ridershipRowKey = s"${entry.station}_$currentDate"

          // Check and update total rides table
          val totalRidesResult = totalRidesTable.get(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes(totalRidesRowKey)))

          // Check if the row or column exists, initialize if missing
          val currentTotalRides = if (totalRidesResult.isEmpty ||
            !totalRidesResult.containsColumn(Bytes.toBytes("data"), Bytes.toBytes("total_rides"))) {
            println(s"Row or column missing for key: $totalRidesRowKey, initializing to 0")
            0L
          } else {
            Bytes.toLong(totalRidesResult.getValue(Bytes.toBytes("data"), Bytes.toBytes("total_rides")))
          }

          // Calculate the new total rides
          val newTotalRides = currentTotalRides + entry.entry_number.toLong

          // Prepare and execute the Put operation
          val totalRidesPut = new Put(Bytes.toBytes(totalRidesRowKey))
          totalRidesPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("total_rides"), Bytes.toBytes(newTotalRides))
          totalRidesTable.put(totalRidesPut)

          // Check and update ridership table
          val ridershipResult = ridershipTable.get(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes(ridershipRowKey)))
          val currentRidership = if (ridershipResult.isEmpty) 0 else Bytes.toInt(ridershipResult.getValue(Bytes.toBytes("data"), Bytes.toBytes("rides")))
          val newRidership = currentRidership + entry.entry_number

          val ridershipPut = new Put(Bytes.toBytes(ridershipRowKey))
          ridershipPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("station_id"), Bytes.toBytes(entry.station))
          ridershipPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("date"), Bytes.toBytes(entry.date))
          ridershipPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("day"), Bytes.toBytes(dayKey))
          ridershipPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("rides"), Bytes.toBytes(newRidership))
          ridershipPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("station_name"), Bytes.toBytes(entry.station_name))
          ridershipTable.put(ridershipPut)

          println(s"Successfully processed station entry: Station ID = ${entry.station}, Day Key = $dayKey")
          println(s"Total Rides for RowKey $totalRidesRowKey updated to: $newTotalRides")
          println(s"Ridership for RowKey $ridershipRowKey updated to: $newRidership (Added ${entry.entry_number} rides)")
        } catch {
          case e: Exception =>
            println(s"Error processing entry: $entry")
            println(e.getMessage)
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
