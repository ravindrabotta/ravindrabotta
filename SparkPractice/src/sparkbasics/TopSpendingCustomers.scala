package sparkbasics

	import org.apache.spark.SparkContext
	import org.apache.log4j.{Logger, Level}
	
	object TopSpendingCustomers extends App {
	
		Logger.getLogger("org").setLevel(Level.ERROR)
		
		val sc = new SparkContext("local[*]",  "Top 10 Customers")
		val inputRDD = sc.textFile("file:///C:/Users/botta/asdf1.txt")
		val pairRDD = inputRDD.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
		val resultRDD = pairRDD.reduceByKey((x, y) => x + y)
		resultRDD.collect().foreach(println)
		
	//	scala.io.StdIn.readLine()
	
}