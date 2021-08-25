package sparkdepth

import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object AmountSpentOnWords extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "AmountPerWord")
  val inputRDD = sc.textFile("file:///D:/TrendyTech/Week_10/bigdatacampaigndata.csv")
  val pairRDD = inputRDD.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  val flatMapRDD = pairRDD.flatMapValues(x => x.split(" "))
  val lowerCaseRDD = flatMapRDD.map(x => (x._2.toLowerCase, x._1))
  val countRDD = lowerCaseRDD.reduceByKey((x, y) => x + y)
  val sortedRDD = countRDD.sortBy(x => x._2, false)

  sortedRDD.collect.foreach(println)

  //scala.io.StdIn.readLine()

}