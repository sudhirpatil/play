package com.sp.plalyground.urbanforest


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

  @transient lazy val sc = spark.sparkContext
  import spark.implicits._
  import org.apache.spark.sql.functions._

  spark.sparkContext.setLogLevel("WARN")
  // Create RDD Multipolygons from urban forest text file
  val forestDf = spark.read.
    option("quote", "\"").
    option("escape", "\"").
    option("sep", " ").
    option("ignoreLeadingWhiteSpace", true).
    csv("/Users/sudhirpatil/code/urbanforest/src/main/Resources/challenge-urban-forest/melb_urban_forest_2016.txt/part-00000")
  forestDf.show(1,false)
  forestDf.printSchema()

  // get RDD (index, Multipolygons)
  import au.com.eliiza.urbanforest._
  //: RDD[(String, MultiPolygon)]
  val forestPolygons = forestDf.limit(100).rdd.map(row => {
    val index = row.getString(0)
    val polyPoints = row.getString(1)
    val pattern = """\(.*?\)""".r
    val polygon: Polygon = pattern.
      findAllIn(polyPoints).map(x => x.replace("(","").replace(")", "")). // Get iterator of each line (sequence of double)
      map(loopStr => {
        // Get Seq(Point) in each line
        val loop: Seq[Point] = loopStr.split(" ").
          map(_.toDouble).toSeq.
          //          combinations(2).
          grouped(2).
          map(x => {
            // Create Point from sequence pair
            val point : Point = x
            //            println(point)
            point
          }).toList
        //        println(loop(0) + "--" + loop(loop.size -1) )
        loop
      }).toList
    val multiPolygon: MultiPolygon = Seq(polygon)
    (index, multiPolygon)
  })
  //  forestPolygons.foreach(row => println("xx"))
  //  val forestDf = forestPolygons.toDF("index", "forest_mpolygon")

  // get multipolygons for each area at level 2 of stat area
  val statAreaDf = spark.read.json("/Users/sudhirpatil/code/urbanforest/src/main/Resources/challenge-urban-forest/melb_inner_2016.json")
  statAreaDf.printSchema()
  statAreaDf.show(1, false)
  // agg at sa2 level
  val statLevel2Df = statAreaDf.groupBy("sa2_main16","sa2_5dig16", "sa2_name16").
    agg(collect_list("geometry.coordinates").alias("seq_multipolygon"))

  import org.apache.spark.sql._
  import spark.implicits._
  // Convert coordinates to Multipolygon from grouped area at sa2
  val statAreaRdd = statLevel2Df.rdd.map(row => {
    val multiPolygons: Seq[MultiPolygon] = row.getSeq[MultiPolygon](row.fieldIndex("seq_multipolygon"))
    val mergePolygon = mergeMultiPolygons(multiPolygons: _*)
    //    println(multiPolygons)
    (row.getString(row.fieldIndex("sa2_main16")), mergePolygon)
  })

  //  statAreaRdd.foreach(println)
  //  forestPolygons.foreach(println)
  // Convert coordinates to Multipolygon from ungrouped stats area
  val statAreaAllRdd = statAreaDf.rdd.map(row => {
    val geoRow = row.getStruct(row.fieldIndex("geometry"))
    val coord: MultiPolygon = geoRow.getSeq[Polygon](geoRow.fieldIndex("coordinates"))
    (row.getString(row.fieldIndex("sa1_main16")), coord)
  })
  //  val areaPolyDf = statAreaRdd.toDF("gcc_code16","stat_mpolygon")

  // RDD with broadcast join
  //  statAreaDf.select("sa1_main16", "geometry.coordinates").rdd.map(row => (row))
  val statAreaBcast = sc.broadcast(statAreaRdd.collectAsMap())
  val greenAreaDf = forestPolygons.flatMap(forestRow => {
    val fmPolygon: MultiPolygon = forestRow._2
//    println(s"Starting forest : ${forestRow._1}")
    statAreaBcast.value.
      filter(statArea => mayIntersect(fmPolygon, statArea._2)).
          map(statArea => (statArea._1, intersectionArea(statArea._2, fmPolygon))).
          filter(statArea => statArea._2  > 0.0 )
  }).toDF("sa2_index", "intersect_area").
    groupBy("sa2_index").sum("intersect_area")

  //try cross join without broadcast, find out overlapping area and keep adding to total count
  // Get green area coverage by joining Stat Area and forest
  val greenAreaRdd = statAreaRdd.cartesian(forestPolygons).map(row => {
    val smp = row._1._2
    val fmp = row._2._2
    val area: Double = if(mayIntersect(smp, fmp)) intersectionArea(smp, fmp) else 0.0
    (row._1._1, area)
  }).filter(_._2 > 0.0)
  println(greenAreaRdd.count())


  //join using dataframe and udf
  val udf: UserDefinedFunction = org.apache.spark.sql.functions.udf((sa : MultiPolygon, fa : MultiPolygon) => { mayIntersect(sa, fa); });
//  forestDf.crossJoin(areaPolyDf, udf(forestDf("forest_mpolygon"), areaPolyDf("stat_mpolygon")) == true)
//  areaPolyDf.join(areaPolyDf,udf(forestDf("forest_mpolygon"), areaPolyDf("stat_mpolygon")) == true, "left")




}
