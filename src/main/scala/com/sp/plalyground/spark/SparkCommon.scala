package com.sp.plalyground.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkCommon extends App{
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("test")
//    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._

  val df = spark.read.options(Map("inferSchema"->"true","sep"->"|","header"->"false")).
    csv("file:///Users/sudhirpatil/mbs.csv")
  df.show()

  case class Books(author: String, name: String, category: String)
  val dfText = spark.sparkContext.
    textFile("file:///Users/sudhirpatil/books.csv").
    map(line => line.split(",")).
    map(attribs => Books(attribs(0), attribs(1), attribs(2))).
    toDF()
  dfText.show()

  dfText.
    write.
    format("parquet").
    mode(SaveMode.Overwrite).
    save("file:///Users/sudhirpatil/books-out.csv")


}
