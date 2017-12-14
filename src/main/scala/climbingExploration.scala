package climb

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scalafx.application.JFXApp
import scalafx.Includes._
import scalafx.scene.Scene
import scalafx.scene.chart.ScatterChart
import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import scalafx.scene.layout.TilePane
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths
import scalafx.collections.ObservableBuffer
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray


object ClimbProject extends JFXApp {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder.appName("climb.ClimbProject").master("spark://pandora00:7077").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  //SCHEMAS

 val ascentSchema = StructType(Array(
        StructField("id", IntegerType),
        StructField("user_id", IntegerType),
        StructField("grade_id", IntegerType),
        StructField("notes", StringType),
        StructField("raw_notes", StringType),
        StructField("method_id", IntegerType),
        StructField("climb_type", StringType),
        StructField("total_score", IntegerType),
        StructField("date", DateType),
        StructField("year", IntegerType),
        StructField("last_year", BooleanType),
        StructField("rec_date", DateType),
        StructField("ascent_date", DateType),
        StructField("name", StringType),
        StructField("crag_id", IntegerType),
        StructField("crag_name", StringType),
        StructField("sector_id", IntegerType),
        StructField("sector", StringType),
        StructField("country", StringType),
        StructField("comment", StringType),
        StructField("rating", IntegerType),
        StructField("description", StringType),
        StructField("yellow_id", StringType),
        StructField("climb_try", BooleanType),
        StructField("repeat", BooleanType),
        StructField("exclude_from_ranking", BooleanType),
        StructField("user_recommended", BooleanType),
        StructField("chipped", BooleanType)
/*        StructField("1", StringType),
        StructField("2", StringType),
        StructField("3", StringType),
        StructField("4", StringType),
        StructField("5", StringType),
        StructField("6", StringType),
        StructField("7", StringType),
        StructField("8", StringType),
        StructField("9", StringType),
        StructField("10", StringType),*/
    ))

 val userSchema = StructType(Array(
        StructField("id", IntegerType),
        StructField("first_name", StringType),
        StructField("last_name", StringType),
        StructField("city", StringType),
        StructField("country", StringType),
        StructField("sex", IntegerType),
        StructField("height", IntegerType),
        StructField("weight", IntegerType),
        StructField("started", IntegerType),
        StructField("competitions", StringType),
        StructField("occupation", StringType),
        StructField("sponsor1", StringType),
        StructField("sponsor2", StringType),
        StructField("sponsor3", StringType),
        StructField("best_area", StringType),
        StructField("worst_area", StringType),
        StructField("guide_area", StringType),
        StructField("interests", StringType),
        StructField("birth_date", DateType),
        StructField("presentation", StringType),
        StructField("deactivated", BooleanType),
        StructField("anonymous", BooleanType)
    ))



  val ascents = spark.read.schema(ascentSchema).option("header", true).csv("/users/iwitecki/BigData/Data/ascent.csv")

  val userRaw = spark.read.schema(userSchema).option("header", true).csv("/users/iwitecki/BigData/Data/user.csv")

  val testUser = userRaw.select('id, 'first_name, 'sex, 'started, 'birth_date)
  println("total users = " + testUser.count())
  println("total users w/ birthdate = " + testUser.filter(col("birth_date").isNotNull).count())
//  println("total users w/ started = " + testUser.filter('started =!= 0).count())
//  println("total users w/ height = " + testUser.filter(col("height") =!= 0).count())
//  println("total users w/ weight = " + testUser.filter(col("weight") =!= 0).count())
  
  val userMaxClimbs = ascents.groupBy('user_id).max("grade_id").withColumnRenamed("max(grade_id)", "max_grade")
  val userFirstLog = ascents.groupBy('user_id).min("year").withColumnRenamed("min(year)", "first_log")

  val userMinClimbs = ascents.groupBy('user_id).min("grade_id").withColumnRenamed("min(grade_id)", "min_grade")
 
  userMaxClimbs.show(20)
/*
  val yearsUDF = udf((i:Int) => 2017 - i)

  val userMax = userRaw.join(userMaxClimbs, userRaw("id") === userMaxClimbs("user_id")).drop(col("user_id"))
  val userMin = userMax.join(userMinClimbs, userMax("id") === userMinClimbs("user_id")).drop(col("user_id"))
  val users = userMin.join(userFirstLog, userMin("id") === userFirstLog("user_id")).drop(col("user_id"))
    .withColumn("years_climbing", yearsUDF(col("first_log")))
    .withColumn("growth", (col("max_grade") - col("min_grade")) / col("years_climbing"))
 
  users.select('id, 'max_grade, 'min_grade, 'growth).show(20)


  val regUsers = users
    .filter(col("height") =!= 0)
    .filter(col("weight") =!= 0)
    .filter(col("growth").isNotNull)
    .select('id, 'height, 'weight, 'max_grade, 'growth)
  
  regUsers.show(20)

  val va = new VectorAssembler().setInputCols(Array("height", "weight")).
    setOutputCol("features")
  val withFeatures = va.transform(regUsers)
  withFeatures.show
      
  val lr = new LinearRegression().setLabelCol("max_grade")
  
  val model = lr.fit(withFeatures)
  
  println(model.coefficients+" "+model.intercept)

  println("rmse: " + model.summary.rootMeanSquaredError)
  
  val fitData = model.transform(withFeatures)
  fitData.show()
  
  scatterPredictions(fitData, "max_grade")
  scatterPredictions(fitData, "prediction")

  println("GROWTH LINEAR REGRESSION")

  val lrGrowth = new LinearRegression().setLabelCol("growth")
  
  val modelGrowth = lrGrowth.fit(withFeatures)
  
  println(modelGrowth.coefficients+" "+modelGrowth.intercept)

  println("rmse: " + modelGrowth.summary.rootMeanSquaredError)
  
  val fitDataGrowth = modelGrowth.transform(withFeatures)
  fitDataGrowth.show()

  scatterPredictions2(fitDataGrowth, "growth")
  scatterPredictions2(fitDataGrowth, "prediction")

// CLUSTERING 
*/  
  val Array(data, notUsed) = ascents.select('user_id, 'name).randomSplit(Array(0.1, 0.9)) 
  println(data.filter('name === "?").count())
  val clusterData = data.na.drop.map(row => (row.getInt(0), row.getString(1))).rdd


  val groupedClimbsDF = data.na.drop.groupBy('user_id).agg(collect_list("name") as "climbs") 
  val groupedClimbs = groupedClimbsDF.select('climbs).rdd.map(row => row(0).asInstanceOf[WrappedArray[String]].toArray.toSeq)  

  val nodes = clusterData.map(_._2).collect.distinct.toSeq.zipWithIndex.map { case (n, i) => i.toLong -> n.trim }
  val nodesMap = nodes.toMap
  val indexMap = nodes.map { case (i, n) => n -> i }.toMap


  val edges = groupedClimbs.flatMap( seq => seq.combinations(2)).flatMap{ list =>

        try { //Error thrown here from indexMap
            val a1 = indexMap(list(0))
            val a2 = indexMap(list(1))
            if(a1 != 1 && a2 !=1) {
                Seq( Edge(a2, a1, 0),
                    Edge(a1, a2, 0))
            } else {
                Seq(Edge(0, 1, 0))
            }
        } catch {
            case _: Throwable => Seq(Edge(0, 1, 0)) 
        }
    }

  println("Testing nodes and edges")
  nodes.take(10) foreach println
//  groupedClimbsDF.show(10)
  groupedClimbs.take(5) foreach println
  println("edges")
  edges.take(10) foreach println 


  val climbGraph = Graph(sc.parallelize(nodes), edges)

  //Page Rank
  climbGraph.pageRank(0.01).vertices.sortBy(_._2, ascending = false).take(10).map(a => (a._2, nodesMap((a._1).toInt))) foreach println

  def scatterPredictions(plotData: Dataset[Row], name: String) {

    val height = plotData.select('height).as[Double].collect()
    val weight = plotData.select('weight).as[Double].collect()
    val point = plotData.select(col(name)).as[Double].collect()

    val cg = ColorGradient(0.0 -> GreenARGB, 30.0 -> YellowARGB, 50.0 -> BlueARGB, 60.0 -> RedARGB, 70.0 -> BlackARGB)
    val plotScatter = Plot.scatterPlot(height, weight, name , "Height", "Weight", 3.0, point.map(cg))
    FXRenderer(plotScatter)

  }

  def scatterPredictions2(plotData: Dataset[Row], name: String) {

    val height = plotData.select('height).as[Double].collect()
    val weight = plotData.select('weight).as[Double].collect()
    val point = plotData.select(col(name)).as[Double].collect()

    val cg = ColorGradient(0.0 -> GreenARGB, 1.0 -> RedARGB, 2.0 -> BlueARGB, 3.0 -> RedARGB, 4.0 -> BlackARGB)
    val plotScatter = Plot.scatterPlot(height, weight, name , "Height", "Weight", 3.0, point.map(cg))
    FXRenderer(plotScatter)

  }

  spark.stop

}


