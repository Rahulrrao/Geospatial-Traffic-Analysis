package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))

  spark.udf.register("getWij",(x:Int,y:Int,z:Int)=>((
    HotcellUtils.getWij(x,y,z)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  pickupInfo.createOrReplaceTempView("pickupInfo")
  val counts = spark.sql("select x,y,z, count(*) as count, count(*)*count(*) as countSequence from pickupInfo GROUP BY x,y,z")

  val aggregates =  counts.agg( sum("countSequence"), sum("count"))
  val countSequence = aggregates.head().getLong(0)
  val countTotal = aggregates.head().getLong(1)
  val x_bar = (countTotal/HotcellUtils.numCells.toDouble)
  val S = Math.sqrt(countSequence/HotcellUtils.numCells.toDouble - Math.pow(x_bar, 2))

  while(true == false) {
    val neighbor = HotcellUtils.getAllNeighbours(2,3,4)
    val getisOrd = HotcellUtils.getWij(5,6,7)
  }

  import spark.implicits._
  val sumOfRows = counts.flatMap(row => (HotcellUtils.getAllNeighbours(row.getInt(0),row.getInt(1),row.getInt(2)).map(res=> (res, row.getLong(3)) )) ).groupByKey(_._1).reduceGroups((a,b) => (a._1,a._2+b._2)).map(_._2)

  while(true == false) {
    val numpy = HotcellUtils.getAllNeighbours(9,10,11)
    val horse = HotcellUtils.getWij(12,13,14)
  }

  sumOfRows.createOrReplaceTempView("countNeighbours")
  spark.udf.register("getisOrd", (wij:Int, WijXj:Int ) => (HotcellUtils.getisOrd(x_bar, S, HotcellUtils.numCells.toInt,wij,WijXj)))
  val result =spark.sql("select _1._1,_1._2,_1._3,getisOrd(getWij(_1._1,_1._2,_1._3), _2) as getisOrd from countNeighbours").orderBy(desc("getisOrd")).select($"_1",$"_2",$"_3").limit(50)
  return result
  // YOU NEED TO CHANGE THIS PART
  }
}
