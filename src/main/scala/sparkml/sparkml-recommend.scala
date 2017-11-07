package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.recommendation.ALS
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SparkRecommend extends App {

  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.OFF)


  val ratingsSchema = StructType(Array(
        StructField("userid", IntegerType),
        StructField("rating", IntegerType),
        StructField("date", StringType),
        StructField("movieid", IntegerType)
        ))

  val movieTitleSchema = StructType(Array(
        StructField("movieid", IntegerType),
        StructField("releaseYear", IntegerType),
        StructField("title", StringType)
        ))
  
  val rating = spark.read.schema(ratingsSchema).option("header", true).option("delimiter", ",").csv("file:///users/iwitecki/BigData/Data/parsed_ratings_1.txt")


  val movies = spark.read.schema(movieTitleSchema).option("header", true).option("delimiter", ",").csv("/data/BigData/Netflix/movie_titles.csv")


  val testData = rating.filter($"movieid" <= 1000)

//IN CLASS

  //Question 1 Range of user ids

  testData.describe("userid").show() 

  //Question 2 Count of distinct user ids

  println(testData.select("userid").distinct().count()) 


  //Question 3 Five Star Ratings of user 372233

  println(testData.filter('userid === 372233 && 'rating === 5).count())

  //Question 4 Movie with most user ratings
  val maxMovie = testData.groupBy('movieid).count.orderBy(desc("count")).limit(1)
  maxMovie.join(movies, "movieid").show(false)

  //Question 5 Movie with most 5 star ratings

  val fiveMovie = testData.filter('rating === 5).groupBy('movieid).count.orderBy(desc("count")).limit(1)
  fiveMovie.join(movies, "movieid").show(false)

//OUT OF CLASS

 //Part 1
/* val data = rating.filter($"userid" <= 100000 && $"movieid" <= 5000)
 
 val als = new ALS()
              .setImplicitPrefs(false)
              .setRank(10)
              .setRegParam(0.1)
              .setAlpha(1.0)
              .setMaxIter(10)
              .setUserCol("userid")
              .setItemCol("movieid")
              .setRatingCol("rating")
  val model = als.fit(data)
  
  val recommendations = model.recommendForAllUsers(5)
  recommendations.limit(5).show(false)

  val detuple = udf{(r:Row)  => r.getInt(0)}


  recommendations.withColumn("recommendations", explode(col("recommendations")))
  	.withColumn("movieid", detuple(col("recommendations")))
	.groupBy('movieid).count.orderBy(desc("count"))
	.limit(10)
	.join(movies, "movieid")
	.show(false)

  //Part 2
  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")

  val Array(training, test) = data.randomSplit(Array(0.8, 0.2))
  
  val testModel = als.fit(training)
  testModel.setColdStartStrategy("drop")
  val predictions = testModel.transform(test)
  val rmse = evaluator.evaluate(predictions)
  println(s"Root-mean-square error = $rmse")
 */
spark.stop
}
