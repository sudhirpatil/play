package com.sp.plalyground.urbanforest

import au.com.eliiza.urbanforest.{Loop, MultiPolygon}
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object UrbanForestTrail extends App {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("StructStreamingExample")
  @transient lazy val sparkPartitions = System.getProperty("SparkUtil.sparkPartitions", "50")
  @transient lazy val appName = System.getProperty("SparkUtil.appName", "StructStreamingExample")
  @transient lazy val master = System.getProperty("spark.master", "local[4]")
  @transient lazy val logLevel = System.getProperty("SparkUtil.logLevel", "INFO")

  @transient lazy val spark = getSpark()

  def getSpark(): SparkSession = {
    SparkSession.builder()
      .master(master)
      .appName(appName)
      //      .enableHiveSupport()
      .getOrCreate()
  }



  val sc = spark.sparkContext
  import spark.implicits._
  import org.apache.spark.sql.functions._

//  val rdd = sc.parallelize(
//    Seq(
//      ("first", Array(2.0, 1.0, 2.1, 5.4)),
//      ("test", Array(1.5, 0.5, 0.9, 3.7)),
//      ("choose", Array(8.0, 2.9, 9.1, 2.5))
//    )
//  )
//  rdd.saveAsTextFile("/tmp/test.txt")

//  val df = spark.createDataFrame(rdd)
//  df.show(false)
//  df.write.format("text").save("/tmp/df-text")

  // Create RDD Multipolygons from urban forest text file
  val forestDf = spark.read.
    option("quote", "\"").
    option("escape", "\"").
    option("sep", " ").
    option("ignoreLeadingWhiteSpace", true).
    csv("/Users/sudhirpatil/code/urbanforest/src/main/Resources/challenge-urban-forest/melb_urban_forest_2016.txt")

  forestDf.show(1,false)
  forestDf.printSchema()

  // get RDD (index, Multipolygons)
  val forestPolygons = forestDf.limit(2).rdd.map(row => {
    val index = row.getString(0)
    val polyPoints = row.getString(1)
    val pattern = """\(.*?\)""".r
    import au.com.eliiza.urbanforest._

    val polygon: Polygon = pattern.
      findAllIn(polyPoints).map(x => x.replace("(","").replace(")", "")). // Get iterator of each line (sequence of double)
      map(loopStr => {
        // Get Seq(Point) in each line
        val loop: Loop = loopStr.split(" ").
          map(_.toDouble).toSeq.
          combinations(2).
          map(x => {
            // Create Point from sequence pair
            val point : Point = x
            point
          }).toSeq
        loop
      }).toSeq
    val multiPolygon: MultiPolygon = Seq(polygon)
    (index, multiPolygon)
  })

  // get multipolygons for each area at level 2 of stat area
  val statAreaDf = spark.read.json("/Users/sudhirpatil/code/urbanforest/src/main/Resources/challenge-urban-forest/melb_inner_2016.json")
  statAreaDf.printSchema()

  // Convert coordinates to Multipolygon
  import au.com.eliiza.urbanforest._
  statAreaDf.limit(2).rdd.map(row => {
    val geoRow = row.getStruct(row.fieldIndex("geometry"))
//    val coordinates = geoRow.getStruct(geoRow.fieldIndex("coordinates"))
//    val coord = geoRow.getValuesMap(Seq("coordinates"))
    val coord: Seq[Polygon] = geoRow.getSeq[Polygon](geoRow.fieldIndex("coordinates"))
    println(multiPolygonArea(coord))
//    coord.foreach(item => println(item))
    row.getString(row.fieldIndex("gcc_code16"))
  }).foreach(println)

  case class Area(areasqkm16: Double, gcc_code16:String, gcc_name16:String, sa1_7dig16:String, sa1_main16:String, sa2_5dig16:String,
                  sa2_main16:String, sa2_name16:String, sa3_code16:String, sa3_name16:String, sa4_code16:String, sa4_name16:String,
                  ste_code16:String, ste_name16:String, geo_type: String, multiPolygon: MultiPolygon)
  statAreaDf.limit(2).rdd.foreach(println)

  statAreaDf.limit(2).map(row => row.getString(row.fieldIndex("gcc_code16"))).show(false)
  statAreaDf.limit(2).rdd.map(record => {
    val geoIndex = record.fieldIndex("geometry")
//    record.getJavaMap()
  }).foreach(println)
  statAreaDf.select("geometry.type").distinct().show(100, false)

  val str = "POLYGON ((144.94662979354604 -37.82156625621051 144.94663123038046 -37.821568356669154)(144.94663228128138 -37.82156761711071 144.94663592173814 -37.821568335752396))"
//  val pattern = """POLYGON \(\((.*)\)\)""".r
//    val pattern = """POLYGON \((\(.*\))+\)""".r
//  val pattern = """POLYGON \((\(.*?\))+\)""".r
val pattern = """\(.*?\)""".r
//  pattern.findAllIn(str).mkString("---")
  import au.com.eliiza.urbanforest._

  val polygon: Polygon = pattern.
    findAllIn(str).map(x => x.replace("(","").replace(")", "")). // Get iterator of each line (sequence of double)
    map(loopStr => {
      // Get Seq(Point) in each line
      import au.com.eliiza.urbanforest._
      val loop: Loop = loopStr.split(" ").
        map(_.toDouble).toSeq.
        combinations(2).
        map(x => {
          // Create Point from sequence pair
          val point : Point = x
          point
        }).toSeq
      loop
    }).toSeq
  val multiPolygon: MultiPolygon = Seq(polygon)

  pattern.findAllIn(str).matchData foreach {
    m => println(m.group(1)+"--"+m.before+"--"+m.start+"--"+m.end)
  }

//      map{case Seq(x,y) => {
//      val p: Point = Seq(x,y)
//        p
//      }
//      }}
//    for(i <- 0 until pointSeq.length
//    if i % 2 == 0) yield (val p : Point = Seq(pointSeq(i).toDouble,pointSeq(i+1).toDouble))
//    {
//      println(s"$i is ${pointSeq(i)}")
//      val p : Point = Seq(pointSeq(i).toDouble,pointSeq(i+1).toDouble)
//      yield p
//    }
//    (index, polyType, pointSeq)
//  }).foreach(println)

//  forestDf.selectExpr(split("_c1", "")).show(1,false)

//  val df2 = df.map(rec=>Amount(rec(0).toInt, rec(1), rec(2).toInt))
//val splitRdd = textFile.map { s =>
//  val a = s.split("[ |]")
//  val date = Array(a(0) + " " + a(1))
//  (date ++ a.takeRight(10)).mkString("\t")
//}
//val usgPairRDD: RDD[(String, Seq[String])] = usgRDD.map(_.split("\n") match {
//  case Array(x, xs @ _*) => (x, xs)
//})

  // RDD of (id, polygon) pair
  // RDD[(String, Polygon)]
//  val polygonsWithId = lines
//    .map(l => {
//      val (id :: polyStr :: Nil) = l.drop(1).dropRight(1).split('^').toList
//      val polyVerticesStr = polyStr.split('(').apply(1)
//      val polyVertices = polyVerticesStr.split(',')
//        .toList
//        .map(s => s.split(' '))
//        .map(a => Vertex(a(0).toFloat, a(1).toFloat))
//      val polygon = Polygon(polyVertices)
//
//      (id, polygon)
//    })

}
