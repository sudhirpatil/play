package com.sp.plalyground.urbanforest

import org.apache.spark.sql.DataFrame


object UrbanForest {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("StructStreamingExample")
  @transient lazy val appName = System.getProperty("SparkUtil.appName", "StructStreamingExample")
  @transient lazy val master = System.getProperty("spark.master", "local[4]")
  @transient lazy val spark = getSpark()

  import org.apache.spark.sql.SparkSession

  def getSpark(): SparkSession = {
    SparkSession.builder()
    .master(master)
    .appName(appName)
    //      .enableHiveSupport()
    .getOrCreate()
  }

  import org.apache.spark.broadcast.Broadcast
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.expressions.UserDefinedFunction
  import au.com.eliiza.urbanforest._

  @transient lazy val sc = spark.sparkContext
  import spark.implicits._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql._

  def main(args: Array[String]) = {
    if (args.length != 2) {
      println("Insufficient Arguments, please provide: <stat area file path> <forest file path>")
      System.exit(-1)
    }
    val statAreaFilePath = args(0)
    val forestFilePath = args(1)

    // Parse forest text files and get RDD with MultiPolygons
    val forestPolygons: RDD[(String, MultiPolygon)] = parseForestFiles(forestFilePath) //"/Users/sudhirpatil/code/urbanforest/src/main/Resources/challenge-urban-forest/melb_urban_forest_2016.txt/part-00000")
    // Read Stat Area json & agg for coordinates and area at Level 2 Statistical Areas
    val statLevel2Df = getStatAreaLevel2(statAreaFilePath) //"/Users/sudhirpatil/code/urbanforest/src/main/Resources/challenge-urban-forest/melb_inner_2016.json")
    // Convert Stat Area coordinates to Multipolygon
    val statAreaPolygons: RDD[(String, MultiPolygon)] = getStatAreaPolygons(statLevel2Df)
    val greenAreaDf = getGreenAreaInSA(statAreaPolygons, forestPolygons)
    val greenestAreas = getGreenestAreas(statLevel2Df, greenAreaDf)
    greenestAreas.show(false)
  }

  // Parse forest text files and get RDD with MultiPolygons
  def parseForestFiles(forestFilePath: String): RDD[(String, MultiPolygon)] = {
    // Create dataframe from text file
    val forestDf = spark.read.
      option("quote", "\"").
      option("escape", "\"").
      option("sep", " ").
      option("ignoreLeadingWhiteSpace", true).
      csv(forestFilePath)

    // Create RDD Multipolygons from Dataframe
    getForestPolygons(forestDf.limit(10))
  }

  // get RDD (index, Multipolygons) from Dataframe
  def getForestPolygons(forestDf: DataFrame): RDD[(String, MultiPolygon)] ={
    forestDf.rdd.map(row => {
      val index = row.getString(0)
      val polyPoints = row.getString(1)
      // Pattern to Extract each loop in polygon as string
      val pattern = """\(.*?\)""".r
      val polygon: Seq[Loop] = pattern.
        // Extract each loop in polygon as string
        findAllIn(polyPoints).map(x => x.replace("(","").replace(")", "")).
        map(loopStr => {
            // Convert to Seq[Double]
            loopStr.split(" ").map(_.toDouble).toSeq.
            // Get Point = Seq(Double)
            grouped(2).
            // Get Line = Seq[Point]
            toSeq
        }).toSeq
      val multiPolygon: MultiPolygon = Seq(polygon)
      (index, multiPolygon)
    })
  }

  // Read Stat Area json & agg for coordinates and area at sa2 level
  def getStatAreaLevel2(statFilePath: String): DataFrame = {
    spark.read.json(statFilePath).
      groupBy("sa2_main16", "sa2_5dig16", "sa2_name16").
        agg(collect_list("geometry.coordinates").alias("seq_multipolygon"),
          sum("areasqkm16").alias("areasqkm16"))
  }

  // Convert coordinates to Multipolygon from grouped area at sa2
  def getStatAreaPolygons(statLevel2Df: DataFrame): RDD[(String, MultiPolygon)] = {
    statLevel2Df.rdd.map(row => {
      val multiPolygons: Seq[MultiPolygon] = row.getSeq[MultiPolygon](row.fieldIndex("seq_multipolygon"))
      val mergePolygon = mergeMultiPolygons(multiPolygons: _*)
      //    println(multiPolygons)
      (row.getString(row.fieldIndex("sa2_main16")), mergePolygon)
    })
  }

  // Get forest area in each Suburb
  def getGreenAreaInSA(statAreaPolygons:RDD[(String, MultiPolygon)], forestPolygons: RDD[(String, MultiPolygon)]): DataFrame = {
    // Broadcast Stat Area RDD as size small compared to forest data
    val statAreaBcast = sc.broadcast(statAreaPolygons.collectAsMap())

    // First get intersection for each forest & stat area, then agg for each stat area
    forestPolygons.
      flatMap(forestRow => { // Multiple records for each forest & stat area
        val fmPolygon: MultiPolygon = forestRow._2
        // Get intersection area Stat area using broadcast value
        statAreaBcast.value.
          filter(statArea => mayIntersect(fmPolygon, statArea._2)).
          map(statArea => (statArea._1, intersectionArea(statArea._2, fmPolygon))).
          filter(statArea => statArea._2 > 0.0)
      }).
      toDF("sa2_main16", "intersect_area").
      //agg intersect value at each stat area
      groupBy("sa2_main16").agg(sum("intersect_area").alias("green_area"))
  }

  def getGreenestAreas(statLevel2Df: DataFrame, greenAreaDf: DataFrame): DataFrame = {
    statLevel2Df.join(greenAreaDf, "sa2_main16").
      select("sa2_main16", "sa2_5dig16", "sa2_name16", "areasqkm16", "green_area").
      withColumn("green_area_ratio", col("green_area") / col("areasqkm16")).
      orderBy(desc("green_area_ratio"))
  }

}
