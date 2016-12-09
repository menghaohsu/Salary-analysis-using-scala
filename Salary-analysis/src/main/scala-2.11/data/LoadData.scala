import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by x on 12/2/2016.
  */
object LoadData {
  def main(args: Array[String]) {
    val logFile = "/Users/MengHaoHsu/Desktop/Salaries.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)

    def clean(a:Array[String]):Array[String]={
      if(a.length==8) Array(a(0)++","+a(1),a(2),a(3),a(4),a(5),a(6),a(7))
      else a
    }

    def checkEmpty(a: Array[String]): Array[String] ={
      for( i<-1 until 5){
        Try(a(i).toDouble) match {
          case Failure(_) => a(i)="0"
          case Success(_) =>
        }
      }
      a
    }

    def pt(a:(String,Array[Double])):Boolean={
      val b=a._2
      if(b(0)+b(1)+b(2)+b(3)<=50000) false
      else true
    }

    val r1 = sc
      .textFile(logFile)
      .map(line=>(line.split(",")))
      .map(clean)
      .map(checkEmpty)
      .map(a=>("JobTitle:"++ a(0).split(" ").mkString("-") ++" Year:"++a(5),Array(a(1).toDouble,a(2).toDouble,a(3).toDouble,a(4).toDouble,1)))
      .filter(pt)
      .reduceByKey( (a, b) =>(a, b).zipped.map(_ + _) )

    val r2=for(a<-r1) yield {
      val b=a._2
      b(0)=b(0)/b(4)
      b(1)=b(1)/b(4)
      b(2)=b(2)/b(4)
      b(3)=b(3)/b(4)
      (a._1,b)
    }

    val r3=r2.collect()
    import java.io._
    val file = "/Users/MengHaoHsu/Desktop/out.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- r3) {
      writer.write(x._1+" ")
      val a=x._2
      val b=a :+ (a(0)+a(1)+a(2)) :+ (a(0)+a(1)+a(2)+a(3))
      for(i<-0 until b.length) b(i)=(b(i)* 100).round / 100.toDouble
      writer.write("BasePay:"+b(0)+" ")
      writer.write("OverTimePay:"+b(1)+" ")
      writer.write("OtherPay:"+b(2)+" ")
      writer.write("Benefits:"+b(3)+" ")
      writer.write("TotalPay:"+b(5)+" ")
      writer.write("TotalBenefits:"+b(6))
      writer.write("\n")  // however you want to format it
    }

    writer.close()
    sc.stop()
  }
}