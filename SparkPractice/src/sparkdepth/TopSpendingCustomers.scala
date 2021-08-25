package sparkdepth

import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object TopSpendingCustomers extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "TopSendingCustomers")
  val inputRDD = sc.textFile("file:///C:/Users/botta/asdf1.txt")
  val pairRDD = inputRDD.map(x => (x.split(",")(0), x.split(",")(1).toFloat))
  val totalRDD = pairRDD.reduceByKey(_ + _)
  val premiumCustomers = totalRDD.filter(x => x._2 > 80)
  val doubleRDD = premiumCustomers.map(x => x._2 * 2)
  doubleRDD.collect.foreach(println)
  println(doubleRDD.count)
  
  doubleRDD.coalesce(10, true)

  scala.io.StdIn.readLine()
}