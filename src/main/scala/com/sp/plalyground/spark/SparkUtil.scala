package com.sp.plalyground.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtil {
  def getSpark(): SparkSession = {
    @transient lazy val log = org.apache.log4j.LogManager.getLogger("StructStreamingExample")

    @transient lazy val sparkPartitions = System.getProperty("SparkUtil.sparkPartitions", "50")
    @transient lazy val appName = System.getProperty("SparkUtil.appName", "StructStreamingExample")
    @transient lazy val master = System.getProperty("spark.master", "local[4]")
    @transient lazy val logLevel = System.getProperty("SparkUtil.logLevel", "INFO")

    SparkSession.builder()
      .master(master)
      .appName(appName)
      //    .enableHiveSupport()
      .getOrCreate()
  }

  def receateHiveTable(df: DataFrame, tableName: String, location: String) = {
    val dfSchema = df.schema
    val fields = dfSchema.fields
    var fieldStr = ""
    for(f <- fields){
      fieldStr += f.name + " " + f.dataType.typeName + ","
    }
    val structFields = fieldStr.substring(0, fieldStr.length - 1)

    val spark = getSpark()
    spark.sql(s"Drop TABLE IF EXISTS ${tableName}")

    val createQuery =
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} (${structFields})
         |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerde'
         | WITH SERDEPROPERTIES ('path'='${location}') STORED AS
         | INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
         |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
         |  LOCATION '${location}'
       """.stripMargin
    spark.sql(createQuery)
    saveParquet(df, location)
  }

  def saveParquet(df: DataFrame, storageLocation : String) = {
    df.
      write.
      format("parquet").
      mode("overwrite").
      option("path", "/apps/hive").
      save(storageLocation)
  }

  def saveHivePatition(df: DataFrame, storageLocation: String)= {
    df.write.mode("overwrite").insertInto(storageLocation)
  }

  def saveHive(df: DataFrame, storageLocation: String)= {
    df.write.mode("overwrite").save(storageLocation)
  }

}
