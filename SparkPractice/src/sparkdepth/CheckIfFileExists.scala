package sparkdepth

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
object CheckIfFileExists extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().appName("CheckIfFileExists")
                .master("local[*]")
                .getOrCreate()
                
  val testSchema = StructType(Array(
    StructField("id", StringType),
    StructField("id2", StringType)
  ))
  
  var df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], testSchema)
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val fileExists = fs.exists(new Path("C:/Users/botta/test/*"))
  if (fileExists) {
        df = spark.read.format("csv").schema(testSchema).load("C:/Users/botta/test/*")
  }     
  df.show(false)
  
}