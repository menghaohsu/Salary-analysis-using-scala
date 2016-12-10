package ml

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
// $example off$

object Logistic {

  def main(args: Array[String]): Unit = {

    val logFile = "/Users/MengHaoHsu/Desktop/Salaries.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data=sc.textFile(logFile) // get standard
    val parsedData=data
      .map{
        line => val parts = line.split(",")
          LabeledPoint(parts(0).toDouble, Vectors.dense(parts.slice(1,5).map(x => x.toDouble)))
      }

    val splits = parsedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)
    val model=new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData)

    val labelAndPreds = testData.map { point =>   // get label and prediction
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }


    val trainErr = labelAndPreds.filter(compareWithLabel).count.toDouble/testData.count // ratio of wrong prediction
    print(1-trainErr)
    sc.stop()
  }

  def compareWithLabel(r:(Double,Double)):Boolean={
    r._1 != r._2
  }
}