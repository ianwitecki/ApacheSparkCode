package sparkClass

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator



object SparkClassify extends App {


  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  
        val data = spark.read.option("header",false).option("delimiter", "\t").csv("/data/BigData/admissions/AdmissionAnon.tsv")

  //IN CLASS CODE 


  //Problem 1
  println(data.first.size)
  println(data.count())

  //Problem 2

  data.agg(countDistinct("_c46")).show()

  data.groupBy("_c46").count().show()


  val cols = ((0 to 4) ++ (8 to 46)).map(i => col("_c" + i).cast(DoubleType))

  //Problem 3
  val ddf = data.select(cols:_*)
  val filteredData = ddf.na.fill(-1.0)
  

  //Problem 4
  val assemblerCorr = new VectorAssembler().
    setInputCols(ddf.columns).
    setOutputCol("features")
  val assembledDataCorr = assemblerCorr.transform(filteredData)

  val Row(corrMatrix: Matrix) = Correlation.corr(assembledDataCorr, "features").head
  println(corrMatrix.toString);

  //Problem 5      
  val closeCorrelations = correlatedFields(corrMatrix.numRows)
  closeCorrelations.sortWith((a, b) => a._2 > b._2)
        .map(a => if (a._1 >= 8) (a._1+3, a._2) else (a._1, a._2)).take(4) foreach println


  def correlatedFields(index: Int):ListBuffer[(Int, Double)] = {
        val correlated = new ListBuffer[(Int, Double)]()

        for (i <- 0 until index) {
                        correlated += ((i, Math.abs(corrMatrix.apply(i, index-1))))
        }
        correlated
}


  //OUT OF CLASS CODE


  val assembler = new VectorAssembler().
    setInputCols(ddf.columns.dropRight(1)).
    setOutputCol("features")
  val assembledData = assembler.transform(filteredData)


       val binaryEvaluator = new BinaryClassificationEvaluator()
                .setLabelCol("_c46")
       val multiEvaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("_c46")
                .setPredictionCol("prediction")
                .setMetricName("accuracy")

  //A

  val Array(trainDataA, testDataA) = assembledData.randomSplit(Array(0.8, 0.2)).map(_.cache())
  val multiReturn = classifiers(trainDataA, testDataA, false, multiEvaluator);

  //B
  val binaryFData = filteredData.withColumn("_c46", when(col("_c46") > 1, 1).otherwise(0))
  val assembledBinaryData = assembler.transform(binaryFData)

 val Array(trainDataB, testDataB) = assembledBinaryData.randomSplit(Array(0.8, 0.2)).map(_.cache())

 val binaryReturn = classifiers(trainDataB, testDataB, true, binaryEvaluator);


  //Print Outs for Out Of Class
  println("PART A\n")
  println("Weights")
  printFeatures(multiReturn._1)
  println("Classifications")
  multiReturn._2 foreach println
  println



  println("PART B\n")
  println("Weights")
  printFeatures(binaryReturn._1)
  println("Classifications")
  binaryReturn._2 foreach println



// Classification Function
def classifiers(trainData: Dataset[Row], testData: Dataset[Row], binary: Boolean, evaluator: Evaluator): (Array[Double], ListBuffer[(String, Double)]) =  { 

  val answers = new ListBuffer[(String, Double)]

  //Random Forest
 
  val rfc = new RandomForestClassifier()
                        .setLabelCol("_c46")  
  val rfcModel = rfc.fit(trainData)
  val rfcPredictions = rfcModel.transform(testData)
//  predictions.show()

  val rfcAccuracy = evaluator.evaluate(rfcPredictions)
  answers += (("RandomForestAccuracy",  rfcAccuracy))


  //Decision Tree
  val dt = new DecisionTreeClassifier()
                        .setLabelCol("_c46")  
  val dtModel = dt.fit(trainData)
  val dtPredictions = dtModel.transform(testData)

  val dtAccuracy = evaluator.evaluate(dtPredictions)
  answers += (("DecisionTreeAccuracy", dtAccuracy))

 //Logistic Regression Only works for Binary
  val lr = new LogisticRegression()
                        .setLabelCol("_c46")  

  if (binary) {
  val lrModel = lr.fit(trainData)
  val lrPredictions = lrModel.transform(testData)

  val lrAccuracy = evaluator.evaluate(lrPredictions)
  answers += (("LogisticRegression", lrAccuracy)) 
  }
  
  if (!binary) {
  val ovr = new OneVsRest().setClassifier(lr).setLabelCol("_c46")
  val ovrModel = ovr.fit(trainData)
  val ovrPredictions = ovrModel.transform(testData)

  val ovrAccuracy = evaluator.evaluate(ovrPredictions)
  answers += (("OVR", ovrAccuracy))
  }


  return (( rfcModel.featureImportances.toArray , answers))
}

def printFeatures(featureWeights: Array[Double]): Unit =  {
  featureWeights.zipWithIndex.sortBy(_._1).reverse.take(5) foreach println
}
  
  spark.stop
}

