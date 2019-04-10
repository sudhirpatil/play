package com.sp.plalyground




object MbsTest {
  val spark = SparkUtil.getSpark()

//  val mbsDf = spark.read.option("header", "true").csv("/apps/data/mbs.csv")


  val mbsDf = spark.sql("select * from testdb.security_raw").
    drop("PrefixId", "InsSrc", "UpdSrc")

  import org.apache.spark.sql.functions._
  val mbsNormalized = mbsDf.withColumn("NormalisationDate", lit(current_timestamp()))

  import org.apache.spark.sql.types.TimestampType
  val mbsNew = mbsNormalized.
    withColumn("datediff", months_between(current_timestamp(), unix_timestamp(col("InsDt"), "yyyyMMdd' 'HH:mm:ss").cast(TimestampType))).
    withColumn("IsNew", when(col("datediff") <= 24,true).otherwise(false)).
    drop("datediff")

  mbsNew.orderBy(col("InsDt").desc).select("InsDt", "IsNew").show(false)
  mbsNew.orderBy(col("InsDt")).select("InsDt", "IsNew").show(false)

  val mbsAgg = mbsNew.
    withColumn("issueYear", expr("substring(issdt, 1, 4)")).
    groupBy("prefix", "product", "issueYear").
    agg(min("issamt").alias("minIssamt"), max("issamt").alias("maxIssamt"), sum("issamt").alias("sumIssamt")).
    drop("issueYear")
//    show(false)

  import org.apache.spark.sql.SaveMode
  mbsAgg.write.mode(SaveMode.Overwrite).saveAsTable("testdb.prefix_product_issue_year")

  spark.sql("select * from testdb.prefix_product_issue_year").show

//  issamt
//  prefix_product_issue_year.
//  Prefix, Product and issue year

//  19940101 00:00:00
}
