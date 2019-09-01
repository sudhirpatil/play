package com.sp.plalyground.urbanforest

import au.com.eliiza.urbanforest.Loop
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object UrbanForest extends App {
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

//  val statArea = spark.read.json("/Users/sudhirpatil/code/urbanforest/src/main/Resources/challenge-urban-forest/melb_inner_2016.json")
//  statArea.printSchema()

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

  val forestDf = spark.read.
    option("quote", "\"").
    option("escape", "\"").
    option("sep", " ").
    option("ignoreLeadingWhiteSpace", true).
    csv("/Users/sudhirpatil/code/urbanforest/src/main/Resources/challenge-urban-forest/melb_urban_forest_2016.txt")

  forestDf.show(1,false)
  forestDf.printSchema()
  forestDf.select(explode(split(col("_c1"), """ """))).show(1, false)



  forestDf.limit(2).rdd.map(row => {
    val index = row.getString(0)
    val polyPoints = row.getString(1).split(""" \(\(""")
    val polyType = polyPoints(0)
    import au.com.eliiza.urbanforest.Point
    val points: Iterator[Point] = polyPoints(1).replace("))", "").
      split(" ").
      map(_.toDouble).toSeq.
      combinations(2).
      map(x => {
        val p: Point = x
        p
      })
    (index, polyPoints(0), points)
  }).foreach(println)

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
