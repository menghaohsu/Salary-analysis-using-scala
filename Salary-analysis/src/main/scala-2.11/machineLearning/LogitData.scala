package data

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

object LogitData {

  def clean(a: Array[String]): Array[String] = {
    // recombine splitted name
    if (a.length == 7) Array(a(0) ++ "," + a(1), a(2), a(3), a(4), a(5), a(6))
    else a
  }

  def checkDouble(a: Array[String]): Array[String] = {
    // non-digits to double
    for (i <- 1 until 5) {
      Try(a(i).toDouble) match {
        case Failure(_) => a(i) = "0"
        case Success(_) =>
      }
    }
    a
  }

  def pt(b: Array[Double]): Boolean = {
    // set threshold
    if (b(1) + b(2) + b(3) + b(4) <= 50000) false
    else true
  }

  def convert(a: Array[String]): Array[String] = {
    // set label
    val key1 = List("police", "sherif", "sergeant")
    val key2 = List("anesth", "medical", "nurs", "health", "physician", "orthopedic")
    if (key1.exists(a(0).toLowerCase().contains)) {
      a(0) = "0"
    }
    else if (key2.exists(a(0).toLowerCase().contains)) {
      a(0) = "1"
    }
    a
  }

  def main(args: Array[String]) {

    val logFile = "/Users/MengHaoHsu/Desktop/Salaries.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    //cores
    val sc = new SparkContext(conf)

    val r1 = sc // transform data
      .textFile(logFile)
      .map(line => (line.split(",")))
      .map(clean)
      .map(checkDouble)
      .map(convert)
      .filter(a => (a(0) == "1" || a(0) == "0"))
      .map(a => Array(a(0).toDouble, a(1).toDouble, a(2).toDouble, a(3).toDouble, a(4).toDouble))
      .filter(pt)

    val r2 = r1.collect() // write to file
    import java.io._
    val file = "/Users/MengHaoHsu/Desktop/out.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file1)))
    for (x <- r2) {
      for (i <- 0 until 5) writer.write(x(i).toString + ",")
      writer.write("\n")
    }

    writer.close()
    sc.stop()
  }
}