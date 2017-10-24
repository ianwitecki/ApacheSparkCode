package sparkml
import scala.collection.mutable.ListBuffer
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg._


object SparkML extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
 
  
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._        

  val cdcLines = spark.sparkContext.textFile("/data/BigData/brfss/LLCP2016.asc")

  val columns = StructType(Array(
        StructField("startColumn", IntegerType),
        StructField("varName", StringType),
        StructField("length", IntegerType)
   ))
  

  var colLines = spark.read.schema(columns).option("header", true).option("delimiter", "\t").csv("/data/BigData/brfss//Columns.txt")

  var cols = colLines.collect.map{ row =>
        (row.getInt(0), row.getString(1), row.getInt(2))
  }
  
  var varNames = cols.map(tuple => tuple._2)

  cols.take(5) foreach println

  var data = cdcLines.map{ line =>
                Row(cols.map{ col =>
                        try {
                                line.substring(col._1, col._1 + col._3).toDouble
                        } catch {
                                case _: NumberFormatException => -1
                        }
                }:_*)
  }

  var dataSchema = StructType{
                cols.map{ col => StructField(col._2, DoubleType) }
        }

  var dataF = spark.createDataFrame(data, dataSchema)

//In Class Problems
 /* 
  println("GENHLTH \n")
  dataF.describe("GENHLTH").show()

  println("PHYSHLTH \n")
  dataF.describe("PHYSHLTH").show()

  println("MENTHLTH \n")
  dataF.describe("MENTHLTH").show()

  println("POORHLTH \n")
  dataF.describe("POORHLTH").show()

  println("EXERANY2 \n")
  dataF.describe("EXERANY2").show()

  println("SLEPTIM1 \n")
  dataF.describe("SLEPTIM1").show()
*/
  
//Out of Class Problems

  var dataArr = cdcLines.map{ line =>
                Array(cols.map{ col =>
                        try {
                                line.substring(col._1, col._1 + col._3).toDouble
                        } catch {
                                case _: NumberFormatException => -1
                        }
                }:_*)
  }

 val vectorData = dataArr.map(arr => Vectors.dense(arr))
 val corrMatrix = Statistics.corr(vectorData, "pearson")

 val genHealthRelated = correlatedFields("MENTHLTH")

 genHealthRelated foreach println       

def correlatedFields(name: String):ListBuffer[String] = {
        val colIndex = varNames.indexOf(name)
        val correlated = new ListBuffer[String]()

        for (i <- 0 until varNames.size) {
                if (corrMatrix.apply(i, colIndex) > 0.06 && corrMatrix.apply(i, colIndex) != 1.0) {
                        correlated += varNames(i)
                }
        }
        correlated
}
  
  
} 


