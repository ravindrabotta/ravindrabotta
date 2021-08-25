package sparkdepth

import org.apache.spark.SparkContext
import org.apache.log4j.{Logger, Level}

object ReduceWorking extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "ReduceWorking")
  val a = 1 to 100
  val base = sc.parallelize(a)
  base.reduce((x, y) => x + y)
  
}