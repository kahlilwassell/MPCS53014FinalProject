import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Increment, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

object StreamStationEntries {

  // JSON Object Mapper
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  // HBase Configuration
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  // Use the following two lines if you are building for the cluster
  hbaseConf.set("hbase.zookeeper.quorum","mpcs530132017test-hgm1-1-20170924181440.c.mpcs53013-2017.internal,mpcs530132017test-hgm2-2-20170924181505.c.mpcs53013-2017.internal,mpcs530132017test-hgm3-3-20170924181529.c.mpcs53013-2017.internal")
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
  val hbaseConnection: Connection = ConnectionFactory.createConnection(hbaseConf)
  val table: Table = hbaseConnection.getTable(TableName.valueOf("kjwassell_cta_total_rides_by_day_hbase"))

  // Case class to map Kafka messages
  private case class StationEntry(station: String, entry_number: Int)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: StreamStationEntries <brokers>
           |  <brokers> is a list of one or more Kafka brokers
           """.stripMargin
      )
      System.exit(1)
    }

    val Array(brokers) = args

    // Spark Streaming Context
    val sparkConf = new SparkConf().setAppName("StreamStationEntries")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Kafka Parameters and Topic
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicsSet = Set[String]("kjwassell_station_entries")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    // Parse Kafka messages into StationEntry objects
    val stationEntries = messages.map(_._2).map(record => mapper.readValue(record, classOf[StationEntry]))

    // Process station entries and update HBase
    stationEntries.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        partition.foreach { entry =>
          try {
            // Row key: station ID + day of the week
            val dayOfWeek = java.time.LocalDate.now.getDayOfWeek.toString.take(2) // Example: "Mo", "Tu"
            val rowKey = s"${entry.station}_$dayOfWeek"

            // Check if the row already exists
            val increment = new Increment(Bytes.toBytes(rowKey))
            increment.addColumn(Bytes.toBytes("data"), Bytes.toBytes("total_rides"), entry.entry_number.toLong)
            increment.addColumn(Bytes.toBytes("data"), Bytes.toBytes("num_days"), 1L)

            table.increment(increment)
            println(s"Updated HBase for rowKey=$rowKey with entry_number=${entry.entry_number}")
          } catch {
            case e: Exception => println(s"Error updating HBase for station ${entry.station}: ${e.getMessage}")
          }
        }
      }
    }

    // Start the Streaming Context
    ssc.start()
    ssc.awaitTermination()
  }
}

