package sparkdepth

import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object LogLevelGrouping extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "LogLevelGrouping")
  val inputRDD = sc.textFile("file:///D:/TrendyTech/Week_10/bigLog.txt")
  val pairRDD = inputRDD.map(x => {
    val fields = x.split(":")
    (fields(0), fields(1))
  })

  pairRDD.groupByKey.collect().foreach(x => (println(x._1, x._2.size)))
  
  val pairRDD1 = inputRDD.map(x => {
    val fields = x.split(":")
    (fields(0), 1)
  })
  pairRDD1.reduceByKey((x,y) => x + y).collect().foreach(x => (println(x._1, x._2)))
  scala.io.StdIn.readLine()

}