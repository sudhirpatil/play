package com.sp.plalyground.practise.hackerrank

import org.apache.spark.sql.SparkSession

object WordCount extends App {
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

  val textFile = spark.sparkContext.textFile("hdfs://apps/temp.txt") //("play.iml")

  //word count
  val counts = textFile.flatMap(line => line.split(" ")).
    map(word => (word, 1)).
    reduceByKey(_ + _)

  counts.foreach(println)
  System.out.println("Total words: " + counts.count());
}
