import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import scala.util.parsing.json.JSON

object UpdateHBaseWithKafka {

  def main(args: Array[String]): Unit = {

    // Spark Streaming context
    val spark = SparkSession.builder
      .appName("UpdateHBaseWithKafka")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "cta-rides-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Kafka topic
    val topics = Array("kjwassell_station_entries")

    // Stream from Kafka
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // HBase table configuration
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", "hbase-cluster")
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    val hbaseTableName = "kjwassell_cta_total_rides_by_day_hbase"

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { partition =>
          val connection = ConnectionFactory.createConnection(hbaseConfig)
          val table = connection.getTable(TableName.valueOf(hbaseTableName))

          partition.foreach { record =>
            val message = record.value()

            // Parse the JSON message
            val json = JSON.parseFull(message).getOrElse(Map.empty[String, Any])
            val station = json.asInstanceOf[Map[String, String]].getOrElse("station", "")
            val entryNumber = json.asInstanceOf[Map[String, String]].getOrElse("entry_number", "1").toInt

            // Get the current day of the week and map it to the table's encoding
            val dayOfWeek = java.time.ZonedDateTime.now(java.time.ZoneId.of("America/Chicago"))
              .getDayOfWeek match {
                case java.time.DayOfWeek.SUNDAY    => "Su"
                case java.time.DayOfWeek.MONDAY    => "M"
                case java.time.DayOfWeek.TUESDAY   => "T"
                case java.time.DayOfWeek.WEDNESDAY => "W"
                case java.time.DayOfWeek.THURSDAY  => "Th"
                case java.time.DayOfWeek.FRIDAY    => "F"
                case java.time.DayOfWeek.SATURDAY  => "S"
              }

            // HBase row key
            val rowKey = s"${station}_${dayOfWeek}"

            // Check if the row exists
            val get = new Get(Bytes.toBytes(rowKey))
            val result = table.get(get)

            if (!result.isEmpty) {
              // Row exists, update fields
              val totalRides = Bytes.toLong(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("total_rides")))
              val numDays = Bytes.toInt(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("num_days")))

              val newTotalRides = totalRides + entryNumber
              val newNumDays = numDays + 1

              val put = new Put(Bytes.toBytes(rowKey))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("total_rides"), Bytes.toBytes(newTotalRides))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("num_days"), Bytes.toBytes(newNumDays))
              table.put(put)

            } else {
              // Row does not exist, create new row
              val put = new Put(Bytes.toBytes(rowKey))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("station_id"), Bytes.toBytes(station))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("day"), Bytes.toBytes(dayOfWeek))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("total_rides"), Bytes.toBytes(entryNumber))
              put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("num_days"), Bytes.toBytes(1))
              table.put(put)
            }
          }

          table.close()
          connection.close()
        }
      }
    }

    // Start the streaming context
    ssc.start()
    ssc.awaitTermination()
  }
}
