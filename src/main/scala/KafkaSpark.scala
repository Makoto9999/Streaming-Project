package com.antlypls.blog


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSpark {

  def row(line: List[String]): Row = {
    Row(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10))
  }


  def main(args: Array[String]): Unit = {
    //Configurations for kafka consumer
    val kafkaBrokers = sys.env.get("KAFKA_BROKERS")
    val kafkaGroupId = sys.env.get("KAFKA_GROUP_ID")
    val kafkaTopic = sys.env.get("KAFKA_TOPIC")


    // Verify that all settings are set
    require(kafkaBrokers.isDefined, "KAFKA_BROKERS has not been set")
    require(kafkaGroupId.isDefined, "KAFKA_GROUP_ID has not been set")
    require(kafkaTopic.isDefined, "KAFKA_TOPIC has not been set")

    // Create Spark Session
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("KafkaSpark")
      .getOrCreate()

    import spark.implicits._

    // Create Streaming Context and Kafka Direct Stream with provided settings and 10 seconds batches
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers.get,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroupId.get,
      "auto.offset.reset" -> "latest"
    )

    val topics = Array(kafkaTopic.get)


    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      //      rdd.foreach { record =>
      val data = rdd.map(record => record.value)
      val txt = data.map(record => Row.fromSeq(record.toString.split(" ").toSeq))
      val txtDF = spark.createDataFrame(txt,
        StructType(
          Seq(
            StructField(name = "IP", dataType = StringType, nullable = true),
            StructField(name = "other1", dataType = StringType, nullable = true),
            StructField(name = "other2", dataType = StringType, nullable = true),
            StructField(name = "other3", dataType = StringType, nullable = true),
            StructField(name = "other4", dataType = StringType, nullable = true),
            StructField(name = "other5", dataType = StringType, nullable = true),
            StructField(name = "other6", dataType = StringType, nullable = true),
            StructField(name = "other7", dataType = StringType, nullable = true),
            StructField(name = "other8", dataType = StringType, nullable = true),
            StructField(name = "other9", dataType = StringType, nullable = true)
          )
        )
      )
      val result = txtDF.groupBy($"IP").agg(count("*").alias("count"))

      result.createOrReplaceTempView("pointtable")
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pointtable AS
          |SELECT *
          |FROM pointtable
          |WHERE count >= 4
          |ORDER BY count DESC
        """.stripMargin
      )
      println("result")
      spark.table("pointtable").show
      if (spark.table("pointtable").count != 0) {
      spark.table("pointtable").write.mode("append").parquet("/result/")
      }
    }
  // Start Stream
  ssc.start()
  ssc.awaitTermination()
  }
}
