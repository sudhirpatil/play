package com.sp.plalyground.practise.hackerrank

import com.sp.plalyground.spark.SparkUtil

object csvExample extends App {
  val spark = SparkUtil.getSpark()

  val booksDf = spark.read.option("header", "true").csv("/apps/data/books.csv")
  booksDf.show()

  booksDf.write.saveAsTable("books")
//  df.write().mode(SaveMode.Append).partitionBy("colname").saveAsTable("Table")
//  hiveContext.setConf("hive.exec.dynamic.partition", "true")
//  hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
  //df.write.partitionBy('year', 'month').saveAsTable(...)

}
