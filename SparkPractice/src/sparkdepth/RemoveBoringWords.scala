package sparkdepth

import scala.io.Source
import scala.collection.mutable._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object RemoveBoringWords extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def loadBoringWords(): Set[String] = {
    val lines = Source.fromFile("D:/TrendyTech/Week_10/boringwords.txt").getLines()
    var boringWords:Set[String] = Set()
    for (line <- lines) {
      boringWords += line
    }
    boringWords
  }

  val sc = new SparkContext("local[*]", "RemoveBoringWords")
  val inputRDD = sc.textFile("file:///D:/TrendyTech/Week_10/bigdatacampaigndata.csv")
  val pairRDD = inputRDD.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  val flatMapRDD = pairRDD.flatMapValues(x => x.split(" "))
  val lowerCaseRDD = flatMapRDD.map(x => (x._2.toLowerCase(), x._1))
  val boringSet = sc.broadcast(loadBoringWords)
  val filteredRDD = lowerCaseRDD.filter(x => !boringSet.value(x._1))
  val sortedRDD = filteredRDD.sortBy(x => x._2, false)

  sortedRDD.collect.foreach(println)

  //scala.io.StdIn.readLine()
}