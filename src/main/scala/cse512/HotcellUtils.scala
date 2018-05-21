package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01
  val minX = -74.50/coordinateStep
  val maxX = -73.70/coordinateStep
  val minY = 40.50/coordinateStep
  val maxY = 40.90/coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def getisOrd(x_bar: Double, S: Double, numCells:Int, wij:Int, WijXj:Int ): Double = {
    val numerator = WijXj-x_bar*wij
    val denominator = S*math.sqrt((numCells*wij-(wij*wij))/(numCells-1))
    return numerator/denominator
  }

  def getWij(x: Int, y:Int, z:Int) : Int = {
    var total = 0
    if (x == minX || x == maxX)total += 1
    if (y == minY || y == maxY)total += 1
    if (z == minZ || x == maxZ)total += 1
    if (total == 0) return 27
    if (total == 1) return 18
    if (total == 2) return 12
    if (total == 3) return 8
    return 0
  }

  def getAllNeighbours(x: Int, y:Int, z:Int): IndexedSeq[(Int,Int,Int)]= {
    val neighbours = for {i <- -1 to 1
                   j <- -1 to 1
                   k <- -1 to 1
                   if z + i <= maxZ
                   if z + i >= minZ
                   if y + j <= maxY
                   if y + j >= minY
                   if x + k <= maxX
                   if x + k >= minX
    } yield (x + k, y + j, z + i)
    return neighbours

  }

  // YOU NEED TO CHANGE THIS PART
}
