package sparkdepth

import org.apache.spark.SparkContext
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession

object RepartitionVsCoalesce extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  case class movies(id: String, name: String, genre: String)
  case class ratings(id: String, c_id: String, rating: String, timestamp: String)
  
  val spark = SparkSession.builder().appName("RepartitionVsCoalesce")
                .master("local[*]").enableHiveSupport().getOrCreate()
  
  val moviesRDD = spark.sparkContext.textFile("file:///D:/TrendyTech/Week_10/movies.txt").map(x => x.split("::"))
  val ratingsRDD = spark.sparkContext.textFile("file:///D:/TrendyTech/Week_10/ratings.txt").map(x => x.split("::"))
  
  import spark.implicits._
  
  val moviesDF = moviesRDD.map{case Array(a0, a1, a2) => movies(a0, a1, a2)}.toDF
  val ratingsDF = ratingsRDD.map{case Array(a0, a1, a2, a3) => ratings(a0, a1, a2, a3)}.toDF
  
  val joinDF = moviesDF.join(ratingsDF, Seq("id"))
  
  val repartitionedDF = joinDF.repartition(20)
  println(repartitionedDF.collect)
  
  val coalescedDF = repartitionedDF.coalesce(10)
  println(coalescedDF.collect)
  
  scala.io.StdIn.readLine()
  
}