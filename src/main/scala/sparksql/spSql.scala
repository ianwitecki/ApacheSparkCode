package sparksql
import scalafx.application.JFXApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SparkSql{


  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.OFF)
      

    val spark = SparkSession.builder().master("spark://pandora00:7077").getOrCreate()
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("WARN")
    val schema = StructType(Array(
        StructField("seriesID", StringType),
        StructField("year", IntegerType),
        StructField("period", StringType),
        StructField("value", DoubleType),
        StructField("footnote", StringType)
    ))
    
    val schema2 = StructType(Array(
        StructField("seriesID", StringType),
        StructField("areaCode", StringType)
    ))


    val schemaSeries = StructType(Array(
        StructField("seriesID", StringType),
        StructField("area_type_code", StringType),
        StructField("area_code", StringType),
        StructField("measure_code", StringType),
        StructField("seasonal", StringType),
        StructField("srd", StringType)
    ))

    val data = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.38.NewMexico")
    val data2 = spark.read.schema(schema2).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.series")
    val texas = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.51.Texas")
    val allStates = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")
    
    val regions = spark.read.schema(schemaSeries).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.series")


//IN CLASS 
    //1
    println(data.select('seriesID).distinct().count())
    //2
    data.filter(substring('seriesID, 19, 2) === "04").orderBy(desc("value")).show()

    //3
    println(data.join(data2, "seriesID").filter('areaCode === "G").count())
    //4
    
    data.filter(substring('seriesID, 19, 2) === "03" && 'year === "2017").groupBy('period).agg(avg('value)).show()
    data.filter(substring('seriesID, 19, 2) === "03" && 'year === "2017").agg(avg('value)).show()
       
//OUT OF CLASS

    //1A
    val laborForce = data.filter(substring('seriesID, 19, 2) === "06" && 'year === "2017")
    .withColumn("seriesID", substring('seriesID, 0, 19)).withColumnRenamed("value", "lf")
    val weightedAvg = data.filter(substring('seriesID, 19, 2) === "03" && 'year === "2017")
    .withColumn("seriesID", substring('seriesID, 0, 19))
    .join(laborForce,Seq("seriesID", "period")) 
    .agg(sum('value * 'lf) / sum('lf)).show()

    
   //1B
   /*The wieghted average is less than the other two averages, however the weighted average is more accurate becuase it accounts for the population of the labor force. This makes it so that if there was a 30 percent unemployment rate in a town of 20 people it would mean far less than it would in a regular average. */

   //2
   
    val texaslf = texas.filter(substring('seriesID, 19, 2) === "06")
    .withColumn("seriesID", substring('seriesID, 0, 19)).withColumnRenamed("value", "lf")
    texas.filter(substring('seriesID, 19, 2) === "03")
    .withColumn("seriesID", substring('seriesID, 0, 19))
    .join(texaslf,Seq("seriesID", "period", "year")) 
    .filter('lf >= 10000)
    .orderBy(desc("value")).limit(1)show()
   
   //3

   allStates.createOrReplaceTempView("allD")

  // spark.sql("select seriesID, period, year, value from allD, (select substring(seriesID, 0, 19) as f from allD where substring(seriesID, 19, 2) == '06' and value > 10000) as labor where substring(seriesID, 0, 19) == labor.f and substring(seriesID, 19, 2) == '03' order by value desc limit 1").show()

  //4
 // regions.groupBy('srd).count().orderBy(desc("count")).limit(1)show(); 

  //5
  //allStates.filter(substring('seriesID, 6, 2) != "72" && substring('seriesID, 6, 2) != "02" && substring('seriesID, 6, 2) != "15").show()

//'year === "2000" || 'year === "2005" || 'year === "2010" || 'year === "2015")

    
    


    spark.stop()

  }
}
