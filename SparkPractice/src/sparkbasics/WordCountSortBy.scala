package sparkbasics

	import org.apache.spark.SparkContext
	import org.apache.log4j.{Logger, Level}
	import org.apache.spark.api.java.function.ForeachPartitionFunction
	
	object WordCountSortBy extends App {
		
		Logger.getLogger("org").setLevel(Level.ERROR)
		
		val sc = new SparkContext("local[*]", "wordcount")
		val inputRDD = sc.textFile("file:///C:/Users/botta/asdf.txt")
		println("No. of partitions in inputRDD  ::  " + inputRDD.getNumPartitions)
		val wordsRDD = inputRDD.flatMap(x => x.split(" "))
		println("No. of partitions in inputRDD  ::  " + wordsRDD.getNumPartitions)
		val pairRDD = wordsRDD.map(x => (x, 1))
		println("No. of partitions in inputRDD  ::  " + pairRDD.getNumPartitions)
		val countsRDD = pairRDD.reduceByKey((x, y) => x + y)
		println("No. of partitions in inputRDD  ::  " + countsRDD.getNumPartitions)
		val sortedRDD = countsRDD.sortBy(x => x._2, false)
		println("No. of partitions in inputRDD  ::  " + sortedRDD.getNumPartitions)
		
		val results = sortedRDD.collect.take(10)
		println(countsRDD.partitioner)
		for(result <- results) {
			val word = result._1
			val count = result._2
			println(s"$word : $count")
		}
		
}