package di.spark.test

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

/** Find the minimum temperature by weather station */
object MaxPreceipit {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val date = fields(1)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, date, entryType, temperature)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxPreceipit")
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but TMIN entries
    val maxPrecip = parsedLines.filter(x => x._3 == "PRCP")
    
    // Convert to (stationID, temperature)
    val stationprecip = maxPrecip.map(x => (x._1, x._4.toFloat))
    val datetemp = maxPrecip.map(x => (x._2, x._4.toFloat))
    
    // Reduce by stationID retaining the minimum temperature found
    val maxprecipByStation = stationprecip.reduceByKey( (x,y) => max(x,y))
    
    // Collect, format, and print the results
    val results = maxprecipByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       // val date = datetemp.filter(x = > x._2 == 86.89999))
       val formattedTemp = f"$temp%.5f F"
       // println(s"$station max precipitation: $formattedTemp on date $date") 
    }
      
  }
}