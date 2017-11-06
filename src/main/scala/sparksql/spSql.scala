package sparksql
import scalafx.application.JFXApp
import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.renderer._
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.styles.HistogramStyle
import swiftvis2.plotting.styles.BarStyle
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SparkSql extends JFXApp{


    Logger.getLogger("org").setLevel(Level.OFF)
      

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
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
        StructField("srd", StringType),
        StructField("title", StringType)
    ))

        
    val schemaZip = StructType(Array(
        StructField("zip", StringType),
        StructField("lat", DoubleType),
        StructField("long", DoubleType),
        StructField("city", StringType),
        StructField("state", StringType),
        StructField("county", StringType)
    ))

    val data = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.38.NewMexico")
    val data2 = spark.read.schema(schema2).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.series")
    val texas = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.51.Texas")
    val allStates = spark.read.schema(schema).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")
    
    val regions = spark.read.schema(schemaSeries).option("header", true).option("delimiter", "\t").csv("/data/BigData/bls/la/la.series")
    val originMaps = spark.read.schema(schemaZip).option("header", true).option("delimiter", ",").csv("/data/BigData/bls/zip_codes_states.csv")

        val maps = originMaps.groupBy('county, 'state).agg(avg('lat) as "lat" , avg('long) as "long")

        maps.show()
//IN CLASS 
    //1
    println(data.select('seriesID).distinct().count())
    //2
    data.filter(substring('seriesID, 19, 2) === "04").orderBy(desc("value")).limit(1).show()

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

   spark.sql("select seriesID, period, year, value from allD, (select substring(seriesID, 0, 19) as f from allD where substring(seriesID, 19, 2) == '06' and value > 10000) as labor where substring(seriesID, 0, 19) == labor.f and substring(seriesID, 19, 2) == '03' order by value desc limit 1").show()

  //4
  regions.groupBy('srd).count().orderBy(desc("count")).limit(1)show(); 

  //5
  val recentYears = allStates.filter(substring('seriesID, 6, 2) =!= "72" && substring('seriesID, 6, 2) =!= "02" && substring('seriesID, 6, 2) =!= "15" && substring('seriesID, 19, 2) === "03")
        .filter('year === "2000" || 'year === "2005" || 'year === "2010" || 'year === "2015")

   recentYears.show()

   maps.show() 
   
   val myUDF = udf{(s:String) => 
                        if (s.contains(":") && s.contains(",")) {
                                (s.split(":")(1).split(",")(0).dropRight(6).trim, s.split(":")(1).split(",")(1).dropRight(3).trim)       } else {
                                (" ", " ")
                        }
                }


   val counties = regions.filter('area_type_code === "F" && 'measure_code === "03").withColumn("split", myUDF(col("title")))
        .withColumn("state", col("split._2"))
        .withColumn("county", col("split._1"))
        .drop('split)
        
   counties.show()

   val mapData = counties.join(recentYears, "seriesID")
   mapData.show()

   val plotData = mapData.select('seriesID, 'state, 'county, 'year, 'value).join(maps, Seq("state", "county")).select('lat, 'long, 'value, 'year).filter('lat.isNotNull && 'long.isNotNull && 'value.isNotNull)

   plotData.show();

   val d2000 = plotData.filter('year === "2000").collect()
   val d2005 = plotData.filter('year === "2005").collect()
   val d2010 = plotData.filter('year === "2010").collect()
   val d2015 = plotData.filter('year === "2015").collect()

   d2000.take(5) foreach println
   d2005.take(5) foreach println
   d2010.take(5) foreach println
   d2015.take(5) foreach println

   val lat1 = d2000.map(_.getDouble(0))
   val lat2 = d2005.map(_.getDouble(0))
   val lat3 = d2010.map(_.getDouble(0))
   val lat4 = d2015.map(_.getDouble(0))


   val long1 = d2000.map(_.getDouble(1))
   val long2 = d2005.map(_.getDouble(1))
   val long3 = d2010.map(_.getDouble(1))
   val long4 = d2015.map(_.getDouble(1))

   long1.take(5) foreach println
   lat1.take(5) foreach println


   val vals1 = d2000.map(_.getDouble(2))
   val vals2 = d2005.map(_.getDouble(2))
   val vals3 = d2010.map(_.getDouble(2))
   val vals4 = d2015.map(_.getDouble(2))

   vals1.take(5) foreach println

   val cg = ColorGradient((0.0, WhiteARGB), (5.0, GreenARGB), (10.0, RedARGB), (50.0, BlackARGB))

    val plot = Plot.scatterPlotGrid(
        Seq(Seq((long1, lat1, vals1.map(cg), 5), (long2, lat2, vals2.map(cg), 5)),
            Seq((long3, lat3, vals3.map(cg), 5), (long4, lat4, vals4.map(cg), 5))),
        "Plot Grid", "Latitude", "Longitude")
    FXRenderer(plot, 1000, 1000)

    spark.stop()

  
}
