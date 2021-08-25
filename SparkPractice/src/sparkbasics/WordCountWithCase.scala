package sparkbasics

import org.apache.spark.SparkContext
import org.apache.log4j.{Logger, Level}

object WordCountWithCase extends App {
	
		Logger.getLogger("org").setLevel(Level.ERROR)
		
		val sc = new SparkContext("local[*]", "wordcount")
		val inputRDD = sc.textFile("file:///C:/Users/botta/asdf.txt")
		val wordsRDD = inputRDD.flatMap(x => x.split(" ")).map(x => x.toUpperCase())
		val pairRDD = wordsRDD.map(x => (x, 1))
		val countsRDD = pairRDD.reduceByKey((x, y) => x + y)
		countsRDD.collect().foreach(println)
}