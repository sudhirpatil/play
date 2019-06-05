package com.sp.plalyground

import org.apache.spark.sql.SparkSession

object Test extends App{
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("test")
    //    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._

}
