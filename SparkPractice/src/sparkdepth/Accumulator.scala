package sparkdepth

import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger, Level }

object Accumulator extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Accumulator")
  val inputRDD = sc.textFile("file:///D:/TrendyTech/Week_10/samplefile.txt")
  val blankLines = sc.longAccumulator("BlankLinesAccumulator")
  val result = inputRDD.flatMap(line => {
    if (line == "") blankLines.add(1)
    line.split(" ")
  })
  result.saveAsTextFile("accum")
  println("Blank Lines  ::  " + blankLines.value)
}