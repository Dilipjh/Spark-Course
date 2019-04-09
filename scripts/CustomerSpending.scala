package di.spark.test

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object CustomerSpending {
  
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val custID = fields(0).toInt
    val itemID = fields(1)
    
    val amount = fields(2).toFloat
    (custID, itemID, amount)
  }
  
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Customer Spending")
    
    //getting to read the csv file
    val input = sc.textFile("../customer-orders.csv")
    
    // Convert to (cust ID,item Id ,amount) tuples
    val parsedLines = input.map(parseLine)
    
    // remove unwanted item ID
    val data = parsedLines.map(x => (x._1, x._3))
    
    // calculating the value
    val count = data.reduceByKey( (x, y) => (x + y) )
       
    
    //printing
    // results.sorted.foreach(println)
    val remix = count.map( x => (x._2, x._1) )
    val sorted = remix.sortByKey()
    
    //collecting results
    val results = sorted.collect()
    
    results.foreach(println)
    
  }
    
}