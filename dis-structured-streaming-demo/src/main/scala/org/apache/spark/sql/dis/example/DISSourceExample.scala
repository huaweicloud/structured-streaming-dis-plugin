package org.apache.spark.sql.dis.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.dis.source.DISKafkaSourceProvider

object DISSourceExample {
  def main(args: Array[String]): Unit = {
    println("Start DIS Structured Streaming Source demo.")
    if (args.length < 7) {
      println(s"args is wrong, should be [endpoint region ak sk projectId streamName startingOffsets]".stripMargin)
      return
    }

    // DIS GW url
    // Region ID
    // Access Key Id
    // Secret Access Key
    // User ProjectId
    // DIS stream name
    // Starting offsets:  'LATEST'    (Starting with the latest sequenceNumber) 
    //                    'EARLIEST'  (Starting with the earliest sequenceNumber)
    //                    '{"0":23,"1":-1,"2":-2}'  (Use json format to specify the starting sequenceNumber of each partition, -1 indicates the latest, -2 indicates the earliest)
    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets)
    = (args(0), args(1), args(2), args(3), args(4), args(5), args(6))

    val spark = SparkSession
      .builder()
      .appName("Spark structured streaming DIS example")
      .getOrCreate()

    // DIS source
    val sourceDF = spark.readStream
      .format("dis")
      .option(DISKafkaSourceProvider.PROPERTY_ENDPOINT, endpoint)
      .option(DISKafkaSourceProvider.PROPERTY_REGION_ID, region)
      .option(DISKafkaSourceProvider.PROPERTY_AK, ak)
      .option(DISKafkaSourceProvider.PROPERTY_SK, sk)
      .option(DISKafkaSourceProvider.PROPERTY_PROJECT_ID, projectId)
      .option(DISKafkaSourceProvider.PROPERTY_STREAM_NAME, streamName)
      .option(DISKafkaSourceProvider.PROPERTY_STARTING_OFFSETS, startingOffsets)
      .load()

    import spark.implicits._

    // File sink
    val checkpointLocation = "/tmp/dis/checkpoint"
    val outputLocation = "/tmp/dis/output"
    val query = sourceDF.select($"topic", $"partition", $"offset", $"value", $"key")
      .as[(String, String, String, String, String)]
      .map(kv => kv._1 + " " + kv._2 + " " + kv._3 + " " + kv._4 + " " + kv._5)
      .as[String]
      .writeStream
      .format("text")
      .option("checkpointLocation", checkpointLocation)
      .start(outputLocation)

    // Console sink
    //    val query = sourceDF.select($"topic", $"partition", $"offset", $"value", $"key")
    //      .as[(String, String, String, String, String)]
    //      .map(kv => kv._1 + " " + kv._2+ " " + kv._3+ " " + kv._4+ " " + kv._5)
    //      .as[String]
    //      .writeStream
    //      .format("console")
    //      .start()

    query.awaitTermination()
  }
}
