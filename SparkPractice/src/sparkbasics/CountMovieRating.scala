package sparkbasics

import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object CountMovieRating extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "CountMovieRating")
  val inputRDD = sc.textFile("file:///C:/Users/botta/asdf1.txt")
  val ratingRDD = inputRDD.map(x => x.split("\t")(2))
  val pairRDD = ratingRDD.map(x => (x, 1))
  val countRatingRDD1 = pairRDD.reduceByKey((x, y) => x + y)
  val countRatingRDD2 = ratingRDD.countByValue() 
  countRatingRDD1.collect.foreach(println)
  countRatingRDD2.foreach(println)
  
  //scala.io.StdIn.readLine()
}