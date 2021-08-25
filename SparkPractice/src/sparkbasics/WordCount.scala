package sparkbasics

import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]) {
    
    val sc = new SparkContext("local[*]","wordcount")
    val inputRDD = sc.textFile("file:///C:/Users/botta/asdf.txt")
    val wordsRDD = inputRDD.flatMap(x => x.split(" "))
    val pairRDD = wordsRDD.map(x => (x, 1))
    val countRDD = pairRDD.reduceByKey((x ,y) => x + y)
    countRDD.collect().foreach(println)
    
    scala.io.StdIn.readLine()
  }
}