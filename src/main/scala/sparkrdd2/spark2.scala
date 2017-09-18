package sparkrdd2
import scalafx.application.JFXApp
import scalafx.Includes._
import scalafx.scene.Scene
import scalafx.scene.chart.ScatterChart
import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import scalafx.scene.layout.TilePane
import scalafx.collections.ObservableBuffer
import scala.collection.immutable.Set
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer
import org.apache.spark.rdd.RDD

case class DataDay(station: String, date: Int, element: String, value: Double);
case class Station(station: String, lat: Double, long: Double, elev: Double, state: String, name: String);


object SpecialRDDs extends JFXApp {


        Logger.getLogger("org").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
        val lines1987 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1987.csv")
        val data2017 = lines2017.map { line =>
                                                val d = line.split(",")
                                                DataDay(d(0), d(1).toInt, d(2), d(3).toDouble)
                                     }
        val stationLines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")
        val stations = stationLines.map { line =>
                                                Station(line.substring(0,11), line.substring(13,20).toDouble, line.substring(22,30).toDouble, line.substring(32,37).toDouble, line.substring(38,40), line.substring(41, 71))
                                     }
/*
        // PROBLEM 1
        val byDay = data2017.filter(a => a.element == "TMAX" || a.element == "TMIN").map(a => (a.station, a.date) -> a)
        val dailyAgg = byDay.aggregateByKey((0.0))({ case (diff, a) => if(diff == 0.0) a.value else math.abs(diff - a.value)}, ((x,y) => x+y))

        val maxDiff = dailyAgg.reduce((a,b) => if (b._2 > a._2) b else a)

        println(maxDiff) */

        //PROBLEM 2
//        val minMax =  (s: (Double, Double) /*(min, max)*//, b: DataDay) => if (b.value > s._2) (s._1, b.value) else if (b.value < s._1) (b.value, s._2) else (s._1, s._2)
//        val aggMnMx = (t1: (Double, Double), t2: (Double, Double) /*(min, max)*/) => if (t1._2 - t1._1 > t2._2 - t2._1) t1 else t2
/*        val byStation = data2017.filter(a => a.element == "TMAX" || a.element == "TMIN").map(a => (a.station) -> a)
                                .aggregateByKey((0.0, 0.0))(minMax, aggMnMx)
        val maxDiff2 = byStation.reduce{case ((stat1, (min1, max1)), (stat2, (min2, max2))) => if (max1 - min1 > max2 - min2) (stat1, (min1, max1)) else (stat2, (min2, max2))}


        println(maxDiff2);*/

        //PROBLEM 3
        val usTemps = data2017.filter(a => a.station.substring(0,2) == "US")
        //val tempCounts = usTemps.aggregate((0.0, 0.0, 0.0, 0.0)/*(TMIN_sum, TMAX_sum, TMIN_count, TMAX_count*/)(
        /*  {case((min, max, cmin, cmax), b) => if (b.element == "TMAX") (min, max + b.value, cmin, cmax + 1) else (min + b.value, max, cmin + 1, cmax)},
          {case ((min1, max1, cmin1, cmax1), (min2, max2, cmin2, cmax2)) => (min1 + min2, max2 + max1, cmin1 + cmin2, cmax1 + cmax2)})
        val tminAve = tempCounts._1 / tempCounts._3
        val tmaxAve = tempCounts._2 / tempCounts._4*/
        //val tempVariances = usTemps.aggregate((0.0, 0.0))((a,b) => if (b.element == "TMAX") (a._1 + (Math.pow())))
        /*println("Standard Deviation for US max: " + standardDev(usTemps.filter(_.element == "TMAX")))
        println("Standard Deviation for US min: " + standardDev(usTemps.filter(_.element == "TMIN")))

        // PROBLEM 4

        val ID2017 = lines2017.map(_.split(",")(0)).distinct()
        val ID1987 = lines1987.map(_.split(",")(0)).distinct()
                val remainder = ID2017.intersection(ID1987)
        println(remainder.count()) //13003val remainder = ID2017 intersect ID1987*/

        //OUT OF CLASS PROBLEMS

        //PROBLEM 1
          //A
        val tempsByStat = usTemps.map(a => (a.station) -> a)
        val latLow = stations.filter(_.lat < 35).map(a => (a.station) -> a).join(tempsByStat).map{case (stat, (a, b)) => b}.filter(a => a.element == "TMAX" || a.element == "TMIN")
        val latMid = stations.filter(a => a.lat > 35 && a.lat < 42).map(a => (a.station) -> a).join(tempsByStat).map{case (stat, (a, b)) => b}.filter(a => a.element == "TMAX" || a.element == "TMIN")
        val latHigh = stations.filter(_.lat > 42).map(a => (a.station) -> a).join(tempsByStat).map{case (stat, (a, b)) => b}.filter(a => a.element == "TMAX" || a.element == "TMIN")
/*        println("High Temperature Standard Deviation for US with latitude under 35: " + standardDev(latLow.filter(_.element == "TMAX")))
        println("High Temperature Standard Deviation for US with latitude betwee 35 and 42: " + standardDev(latMid.filter(_.element == "TMAX")))
        println("High Temperature Standard Deviation for US with latitude above 42: " + standardDev(latHigh.filter(_.element == "TMAX")))

          //B

        println("Average Temp Standard Deviation for US with latitude under 35: " + standardDev(averageTemps(latLow)))
        println("Average Temp Standard Deviation for US with latitude betwee 35 and 42: " + standardDev(averageTemps(latMid)))
        println("Average Temp Standard Deviation for US with latitude above 42: " + standardDev(averageTemps(latHigh)))
*/
          //C

        latHistPlots(latLow.filter(a => a.element == "TMAX").map(_.value))
        latHistPlots(latMid.filter(a => a.element == "TMAX").map(_.value))
        latHistPlots(latHigh.filter(a => a.element == "TMAX").map(_.value))

        def latHistPlots(rdd: RDD[Double]) : Unit = {
          val bins = (00.0 to 500.0 by 1.0).toArray
          val hist = rdd.histogram(bins, true)
          val plot = Plot.histogramPlot(bins, hist, RedARGB, false)
          FXRenderer(plot)
        }



        def standardDev(rdd : RDD[DataDay]) : Double = {
          val tempCounts = rdd.aggregate(0.0, 0.0)/*(sum, count)*/(
            {case((sum, count), b ) => (sum + b.value, count + 1)},
            {case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)})
          val ave = tempCounts._1 / tempCounts._2
          val averageSum = rdd.aggregate(0.0)((a,b) => (a + Math.pow(b.value - ave, 2)), (x, y) => x+y)
          Math.sqrt(averageSum/tempCounts._2)
        }

        def averageTemps(rdd: RDD[DataDay]) : RDD[DataDay] = {
          val groupedRdd = rdd.filter(a => a.element == "TMAX" || a.element == "TMIN").map(a => (a.station, a.date) -> a)
          val dailySum = groupedRdd.aggregateByKey((0.0))({ case (diff, a) => /*if(diff == 0.0) a.value else*/ (diff + a.value)}, ((x,y) => x+y))
          dailySum.map{case (stationD, data) => DataDay(stationD._1, stationD._2, "TAVE", data/2)}
        }
}
