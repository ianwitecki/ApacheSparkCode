package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scalafx.application.JFXApp
//import scalafx.Includes._
import scalafx.scene.Scene
import scalafx.scene.chart.ScatterChart
import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import scalafx.scene.layout.TilePane
import scalafx.collections.ObservableBuffer
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._





/**
 * Clustering on the BRFSS data set. https://www.cdc.gov/brfss/
 */
object SparkCluster extends JFXApp {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder.appName("sparkcluster.SparkCluster").master("spark://pandora00:7077").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val data = spark.read.option("header", true).csv("/data/BigData/bls/qcew/2016.q1-q4.singlefile.csv").cache()

  val electionData = spark.read.option("header", true).csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv")

    val schemaZip = StructType(Array(
        StructField("zip", StringType),
        StructField("lat", DoubleType),
        StructField("long", DoubleType),
        StructField("city", StringType),
        StructField("state", StringType),
        StructField("county", StringType)
    ))

 val zipCode = spark.read.schema(schemaZip).option("header", true).option("delimiter", ",").csv("/data/BigData/bls/zip_codes_states.csv")
  val zips = zipCode.groupBy('county, 'state).agg(avg('lat) as "lat" , avg('long) as "long")


  val electionCounties = electionData.filter('county_name =!= "Alaska").withColumn("county", electionData.col("combined_fips").cast(IntegerType))
    .drop("combined_fips")
    .withColumnRenamed("county", "combined_fips")
    .select('state_abbr, 'county_name, 'combined_fips, 'per_dem)

  

    electionCounties.show(20)

  


  //data.show()

  println("/*---------------------(づ｡◕‿‿◕｡)づ---------------------*/")
/*
  // 1. Which aggregation level codes are for county-level data? How many entries are in the main data file for each of the county level codes?
  val intUDF = udf { (s: String) => s.toInt }
  println(data.filter(intUDF(col("agglvl_code")) >= 70 && intUDF(col("agglvl_code")) <= 78).count())

  // 2. How many entries does the main file have for Bexar County?
  println(data.filter(col("area_fips") === "48029").count())

  // 3. What are the three most common industry codes by the number of records? How many records for each?
  data.groupBy(col("industry_code"))
    .count()
    .orderBy(desc("count"))
    .show(4)

  // 4. What three industries have the largest total wages for 2016? What are those total wages? (Consider only NAICS 6-digit County values.)
  val longUDF = udf { (s: String) => s.toLong }
  data.filter(col("agglvl_code") === "78")
    .withColumn("encoded_total_qtrly_wages", longUDF(col("total_qtrly_wages")))
    .groupBy(col("industry_code"))
    .sum("encoded_total_qtrly_wages")
    .orderBy(desc("sum(encoded_total_qtrly_wages)"))
    .show(4)
*/
  println("/*---------------------(ﾉ ｡◕‿‿◕｡)ﾉ*:･ﾟ✧ ✧ﾟ･-------------*/")

  // OUT OF CLASS

  // CLUSTER
  

  val recastedData = data.withColumn("area", data.col("area_fips").cast(IntegerType))
    .drop("area_fips")
    .withColumnRenamed("area", "area_fips")



  val clusterData = data.join(electionCounties, data.col("area_fips") === electionCounties.col("combined_fips")).na.drop

  val columnsToKeep = ("industry_code avg_wkly_wage month1_emplvl month2_emplvl month3_emplvl qtrly_contributions lq_qtrly_estabs").split(" ")

  //clusterData.show(20)

// val columnsToKeep = " ".split("qtrly_estabs month1_emplvl month2_emplvl month3_emplvl total_qtrly_wages taxable_qtrly_wages qtrly_contributions avg_wkly_wage lq_qtrly_estabs lq_month1_emplvl lq_month2_emplvl lq_month3_emplvl lq_total_qtrly_wages lq_taxable_qtrly_wages lq_qtrly_contributions lq_avg_wkly_wage oty_qtrly_estabs_chg oty_qtrly_estabs_pct_chg oty_month1_emplvl_chg oty_month1_emplvl_pct_chg oty_month2_emplvl_chg oty_month2_emplvl_pct_chg oty_month3_emplvl_chg oty_month3_emplvl_pct_chg oty_total_qtrly_wages_chg oty_total_qtrly_wages_pct_chg oty_taxable_qtrly_wages_chg oty_taxable_qtrly_wages_pct_chg oty_qtrly_contributions_chg oty_qtrly_contributions_pct_chg oty_avg_wkly_wage_chg oty_avg_wkly_wage_pct_chg area_fips per_dem").split(" ")


  val typedData = columnsToKeep.foldLeft(clusterData)((df, colName) => df.withColumn(colName, df(colName).cast(IntegerType).as(colName))).na.drop()

  val assembler = new VectorAssembler().setInputCols(columnsToKeep).setOutputCol("features")
  val dataWithFeatures = assembler.transform(typedData)
  //dataWithFeatures.show()

  val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures")
  val normData = normalizer.transform(dataWithFeatures)

  val kmeans = new KMeans().setK(2).setFeaturesCol("normFeatures")
  val model = kmeans.fit(normData)

  val predictions = model.transform(normData)
  predictions.select("features", "prediction").show()

  val areaToPrediction = predictions.groupBy('area_fips).avg("prediction").withColumn("guess", when($"avg(prediction)" >= 0.5, 1).otherwise(0))

  val answerCheck = areaToPrediction.join(electionCounties, data.col("area_fips") === electionCounties.col("combined_fips")).withColumn("real", when('per_dem >= 0.50, 1).otherwise(0))

  val total = answerCheck.count()
  println("Total = " + total)
  val totalCorrect = answerCheck.filter('guess === 'real).count()
  println("Total Right = " +  (totalCorrect.toDouble / total.toDouble))

  
/*  val kmeansTri = new KMeans().setK(3).setFeaturesCol("normFeatures")
  val modelTri = kmeansTri.fit(normData)

  val predictionsTri = modelTri.transform(normData)

  predictionsTri.select("prediction").show(30)

  predictionsTri.agg(countDistinct("prediction")).show()


  val areaToPredictionTri = predictionsTri.groupBy('area_fips).avg("prediction").withColumn("guess", when($"avg(prediction)" < 0.7, 0).when($"avg(prediction)" < 1.3 && $"avg(prediction)" >= 0.7, 1).otherwise(2))

  val answerCheckTri = areaToPredictionTri.join(electionCounties, data.col("area_fips") === electionCounties.col("combined_fips"))
    .withColumn("real", when('per_dem < 0.35, 0).when('per_dem < 0.65 && 'per_dem >= 0.35, 1).otherwise(2))

  answerCheckTri.show(20)

  val totalTri = answerCheckTri.count()
  println("Total 3 Cluster= " + totalTri)
  val totalCorrectTri = answerCheckTri.filter('guess === 'real).count()
  println("Total Right 3 Cluster = " +  (totalCorrectTri.toDouble / totalTri.toDouble)) */

    // Question 2
    
    val myUdf = udf{(s:String) => s.split(" ").dropRight(1).mkString(" ").trim}
    

    
    // Graph for two clusters
    val g2 = answerCheck.withColumn("county", myUdf(col("county_name")))
    g2.show(20)
    val graph2 = g2.select('state_abbr, 'county_name, 'guess, 'county)
        .join(zips, g2("county") === zips("county") && g2("state_abbr") === zips("state"))
    graph2.show(20)

    val long2 = graph2.select('long).map(r => r(0).asInstanceOf[Double]).collect
    val lat2 = graph2.select('lat).map(r => r(0).asInstanceOf[Double]).collect
    val point2 = graph2.select('guess).map(r => r(0).asInstanceOf[Double]).collect

    val cg2 = ColorGradient((0.0, BlueARGB), (1.0, RedARGB))
    val plotScatter = Plot.scatterPlot(long2, lat2, "Two Cluster Votes", "Latitude", "Longitude", 2.0, point2.map(guess => cg2(guess)))
    FXRenderer(plotScatter)

  spark.stop()
}

