package com.sp.plalyground

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructStreamingExample extends App {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("StructStreamingExample")

  @transient lazy val sparkPartitions = System.getProperty("SparkUtil.sparkPartitions", "50")
  @transient lazy val appName = System.getProperty("SparkUtil.appName", "StructStreamingExample")
  @transient lazy val master = System.getProperty("spark.master", "local[4]")
  @transient lazy val logLevel = System.getProperty("SparkUtil.logLevel", "INFO")

  val spark = SparkSession.builder()
    .master(master)
    .appName(appName)
//    .enableHiveSupport()
    .getOrCreate()

  // read from kafka
  val kafkaInputStream = spark.
    readStream.
    format("kafka").
    option("subscribe", "testread").
    option("kafka.bootstrap.servers", "localhost:9092").
    option("startingoffsets", "earliest").
    load()

  kafkaInputStream.printSchema()
  import spark.implicits._
  // cast key, value from binary to String
  val records = kafkaInputStream.
    select(
      $"key" cast "string",
      $"value" cast "string",
      $"topic",
      $"partition",
      $"offset"
    )

//  val records1: Dataset[(String, String)] = kafkaData.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//    .as[(String, String)]

  import org.apache.spark.sql.streaming._
  import scala.concurrent.duration._
  // Write streams to console every 10 secs
  val streamingQueryConsole = records.
    writeStream.
    format("console").
    option("truncate", false).
    trigger(Trigger.ProcessingTime(10.seconds)).
    outputMode(OutputMode.Append()).
    queryName("from-kafka-to-console").
    start()
  streamingQueryConsole.awaitTermination()

  val streamingQueryKafka = records.
    writeStream.
    format("kafka").
    option("topic", "testwrite").
    option("kafka.bootstrap.servers", "localhost:9092").
    option("checkpointLocation", "/tmp/kafka").
    trigger(Trigger.ProcessingTime(10.seconds)).
    start()

  streamingQueryKafka.awaitTermination()
  streamingQueryKafka.stop()

  //read json
  import org.apache.spark.sql.types.{StructType,IntegerType,StringType}
  import org.apache.spark.sql.functions._
//  val schema = StructType().add("a", IntegerType()).add("b", StringType())
  val jsonSchema = spark.sqlContext.read.json("""{"id":"1", "name": "test-name"}""").schema
//  val jsonSchema = new StructType().
//    add($"id".string).
//    add($"name".string)

  val kafkaJson = kafkaInputStream.
    select(from_json(col("value").cast("string"), jsonSchema).alias("valueJson")).
    select("valueJson.*").
//    select(col("value").cast("string")).
    writeStream.
    format("console").
    option("truncate", false).
    trigger(Trigger.ProcessingTime(10.seconds)).
    outputMode(OutputMode.Append()).
    queryName("from-kafka-to-console").
    start()


//  val words = lines.as[String].flatMap(_.split(" "))
//  val wordCounts = words.groupBy("value").count()

//  val windowedCounts = words
//    .withWatermark("timestamp", "10 minutes")
//    .groupBy(
//      window($"timestamp", "10 minutes", "5 minutes"),
//      $"word")
//    .count()


//  txs.writeStream
//    .saveAsTable("dbname.tablename")
//    .format("parquet")
//    .option("path", "/user/hive/warehouse/tx_stream/")
//    .option("checkpointLocation", "/checkpoint_path") .outputMode("append")
//    .start()

//  query = csv_select \
//  .writeStream \
//  .format("csv") \
//  .option("format", "append") \
//  .option("path", "/destination_path/") \
//  .option("checkpointLocation", "/checkpoint_path") \
//  .outputMode("append") \
//  .start()

//  mydataSplitted.foreachRDD( rdd => {
//    println("Processing mydata RDD")
//    val sqlContext = SQLHiveContextSingleton.getInstance( rdd.sparkContext )
//    val mydataDF = sqlContext.createDataFrame( rdd, mydataStruct )
//    mydataDF.registerTempTable("mydata")
//    val mydataTrgPart = sqlContext.sql(mydataSQL)
//    sqlContext.sql("SET hive.exec.dynamic.partition = true;")
//    sqlContext.sql("SET hive.exec.dynamic.partition.mode = nonstrict;")
//    mydataTrgPart.write.mode(SaveMode.Append).partitionBy(partCol).saveAsTable(mydataTable)
//  } )
}
