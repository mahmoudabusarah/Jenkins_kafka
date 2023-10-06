package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Reading_Kafka extends App {
  val spark = SparkSession.builder.appName("KafkaToJson").master("local[*]").getOrCreate()

  // Define the Kafka parameters
  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "ip-172-31-5-217.eu-west-2.compute.internal:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "group1",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false"
  )

  // Define the Kafka topic to subscribe to
  val kafkaTopic = "FraudAPIa"

  // Define the schema for the JSON messages
  val schema = StructType(Seq(
    StructField("age", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("dollar", StringType, nullable = true),
    StructField("firstname", StringType, nullable = true),
    StructField("id", StringType, nullable = true),
    StructField("lastname", StringType, nullable = true),
    StructField("state", StringType, nullable = true),
    StructField("street", StringType, nullable = true),
    StructField("zip", StringType, nullable = true)
  ))
// hellp
  // Read the JSON messages from Kafka as a DataFrame
  val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092").option("subscribe", kafkaTopic).option("startingOffsets", "earliest").load().select(from_json(col("Value").cast("string"), schema).alias("data")).selectExpr("data.*")

  // Write the DataFrame as CSV files to HDFS
  df.writeStream.format("csv").option("checkpointLocation", "/tmp/jenkins/kafka/fraud_api").option("path", "/tmp/jenkins/kafka/fraud_api/FraudApi").start().awaitTermination()


}
