package sparkbasics

import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object AvgConnections extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line: String): (String, Int)= {
    val fields = line.split(",")
    val age = fields(2)
    val num = fields(3).toInt
    (age, num)
  }

  val sc = new SparkContext("local[*]", "AvgConnections")
  val inputRDD = sc.textFile("file:///C:/Users/botta/asdf1.txt")
  val pairRDD = inputRDD.map(parseLine)
  val plusOneRDD = pairRDD.map(x => (x._1, (x._2, 1)))
  val pluOneRDD1 = pairRDD.mapValues(x => (x,1))
  val countRDD = plusOneRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  val avgConnRDD = countRDD.map(x => (x._1, x._2._1 / x._2._2))
  val avgConnRDD1 = countRDD.mapValues(x=> x._1/x._2)
  avgConnRDD.collect.foreach(println)
  avgConnRDD1.collect.foreach(println)
  
  
  
  //scala.io.StdIn.readLine()

}