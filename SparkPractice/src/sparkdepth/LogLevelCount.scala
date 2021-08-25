package sparkdepth

import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object LogLevelCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val logList = List(
    "ERROR: Tuesday 4 September 0405",
    "WARN: Tuesday 4 September 0405",
    "WARN: Tuesday 4 September 0405",
    "WARN: Tuesday 4 September 0405",
    "WARN: Tuesday 4 September 0405",
    "WARN: Tuesday 4 September 0405")

  val sc = new SparkContext("local[*]", "LogLevelCount")
  val inputRDD = sc.parallelize(logList)
  val pairRDD = inputRDD.map(x => {
    val fields = x.split(":")
    val logLevel = fields(0)
    (logLevel, 1)
  })

  val countRDD = pairRDD.reduceByKey((x, y) => x + y)
  countRDD.collect.foreach(println)
  println(countRDD.partitioner)

  sc.parallelize(logList).map(x => (x.split(":")(0), 1)).reduceByKey(_ + _).collect.foreach(println)
  
  scala.io.StdIn.readLine()
}