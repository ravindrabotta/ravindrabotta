package sparkbasics

import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object WordCountSorted extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "wordcount")
  val inputRDD = sc.textFile("file:///C:/Users/botta/asdf.txt")
  val wordsRDD = inputRDD.flatMap(x => x.split(" "))
  val pairRDD = wordsRDD.map(x => (x, 1))
  val countsRDD = pairRDD.reduceByKey((x, y) => x + y)

  val reverseRDD = countsRDD.map(x => (x._2, x._1))
  val sortedRDD = reverseRDD.sortByKey(false)
  sortedRDD.collect().foreach(println)

}