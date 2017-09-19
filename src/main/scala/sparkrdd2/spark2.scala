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

        val lines2016 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2016.csv")
        val lines1897 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1897.csv")
        val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
        val lines1987 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1987.csv")
        val data2017 = fromFy(lines2017)
        val data1897 = fromFy(lines1897)
        val data2016 = fromFy(lines2016)
        val data1987 = fromFy(lines1987)
        val stationLines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")
        val stations = stationLines.map{ line =>
                                                Station(line.substring(0,11), line.substring(12,20).toDouble, line.substring(21,30).toDouble, line.substring(31,37).toDouble, line.substring(38,40), line.substring(41, 71))
                                     }
        def fromFy(lines: RDD[String]): RDD[DataDay] = {
          lines.map { line =>
                            val d = line.split(",")
                            DataDay(d(0), d(1).toInt, d(2), d(3).toDouble)
                       }
        }


        // PROBLEM 1
        val byDay = data2017.filter(a => a.element == "TMAX" || a.element == "TMIN").map(a => (a.station, a.date) -> a)
        val dailyAgg = byDay.aggregateByKey((0.0))({ case (diff, a) => if(diff == 0.0) a.value else math.abs(diff - a.value)}, ((x,y) => x+y))

        val maxDiff = dailyAgg.reduce((a,b) => if (b._2 > a._2) b else a)

        println("In class 1: " + maxDiff)

        //PROBLEM 2
        val minMax =  (s: (Double, Double) /*(min, max)*/, b: DataDay) => if (b.value > s._2) (s._1, b.value) else if (b.value < s._1) (b.value, s._2) else (s._1, s._2)
        val aggMnMx = (t1: (Double, Double), t2: (Double, Double) /*(min, max)*/) => if (t1._2 - t1._1 > t2._2 - t2._1) t1 else t2
        val byStation = data2017.filter(a => a.element == "TMAX" || a.element == "TMIN").map(a => (a.station) -> a)
                                .aggregateByKey((0.0, 0.0))(minMax, aggMnMx)
        val maxDiff2 = byStation.reduce{case ((stat1, (min1, max1)), (stat2, (min2, max2))) => if (max1 - min1 > max2 - min2) (stat1, (min1, max1)) else (stat2, (min2, max2))}


        println("In class 2: " + maxDiff2);

        //PROBLEM 3
        val usTemps = data2017.filter(a => a.station.substring(0,2) == "US")
        val tempCounts = usTemps.aggregate((0.0, 0.0, 0.0, 0.0)/*(TMIN_sum, TMAX_sum, TMIN_count, TMAX_count*/)(
          {case((min, max, cmin, cmax), b) => if (b.element == "TMAX") (min, max + b.value, cmin, cmax + 1) else (min + b.value, max, cmin + 1, cmax)},
          {case ((min1, max1, cmin1, cmax1), (min2, max2, cmin2, cmax2)) => (min1 + min2, max2 + max1, cmin1 + cmin2, cmax1 + cmax2)})
        val tminAve = tempCounts._1 / tempCounts._3
        val tmaxAve = tempCounts._2 / tempCounts._4
        println("Standard Deviation for US max: " + standardDev(usTemps.filter(_.element == "TMAX")))
        println("Standard Deviation for US min: " + standardDev(usTemps.filter(_.element == "TMIN")))

        // PROBLEM 4

        val ID2017 = lines2017.map(_.split(",")(0)).distinct()
        val ID1987 = lines1987.map(_.split(",")(0)).distinct()
                val remainder = ID2017.intersection(ID1987)
        println("In Class 4: " + remainder.count()) //13003val remainder = ID2017 intersect ID1987*/

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

        println("Average Temp Standard Deviation for US with latitude under 35: " + standardDev(averageTemps(latLow, true)))
        println("Average Temp Standard Deviation for US with latitude betwee 35 and 42: " + standardDev(averageTemps(latMid, true)))
        println("Average Temp Standard Deviation for US with latitude above 42: " + standardDev(averageTemps(latHigh, true)))
*/
          //C

        latHistPlots(latLow.filter(a => a.element == "TMAX").map(_.value), "High Temperatures from Latitudes under 35")
        latHistPlots(latMid.filter(a => a.element == "TMAX").map(_.value), "High Temperatures from Latitudes between 35 and 45")
        latHistPlots(latHigh.filter(a => a.element == "TMAX").map(_.value), "High Temperatures from Latitudes over 45")

        def latHistPlots(rdd: RDD[Double], title: String) : Unit = {
          val bins = (0.0 to 500.0 by 1.0).toArray
          val hist = rdd.histogram(bins, true)
          val plot = Plot.histogramPlot(bins, hist, RedARGB, false, title, "Temperature", "Count")
          FXRenderer(plot)
        }
/*
      // PROBLEM 2
      val ave2017 = averageTemps(data2017, false).map(a => a.station -> a).join(stations.map(a => a.station -> (a.lat, a.long)))
      val latvals = ave2017.map{ case (station, (data, location)) => ( data.value, location._1, location._2)}

      val col = latvals.collect.toList
      print(col.length)
      col.take(5) foreach println
      val cg = ColorGradient((0.0, BlueARGB), (100.0, GreenARGB), (500.0, RedARGB))
      val plotScatter = Plot.scatterPlot(col.map{case (temp, lat, long) => long}, col.map{case (temp, lat, long) => lat}, "Temperature By Location", "Latitude", "Longitude", 2.0, col.map{case (temp, lat, long) => cg(temp)})
      FXRenderer(plotScatter)

      //PROBLEM 3
        //A
        val temps1897 = data1897.filter(a => a.element == "TMAX" || a.element == "TMIN")
        val temps2016 = data2016.filter(a => a.element == "TMAX" || a.element == "TMIN")
        println("Average temperature for 1897: " + problem3A(temps1897))
        println("Average temperature for 2016: " + problem3A(temps2016))

        //B
        val sameStat = stats(data1897).intersection(stats(data2016))
        val temps2017 = data2017.filter(a => a.element == "TMAX" || a.element == "TMIN")
        val temps1987 = data1987.filter(a => a.element == "TMAX" || a.element == "TMIN")
        println("Average temperature for 1897 (Common Stations): " + problem3B(temps1897, sameStat.collect.toSet))
        println("Average temperature for 2016 (Common Stations): " + problem3B(temps2016, sameStat.collect.toSet))

        //C

        val data1907 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1907.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data1917 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1917.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data1927 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1927.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data1937 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1937.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data1947 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1947.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data1957 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1957.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data1967 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1967.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data1977 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1977.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data1997 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/1997.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))
        val data2007 = fromFy(sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2007.csv")).filter(a => (a.element == "TMAX" || a.element == "TMIN"))

        val statList = stats(data2007).intersection(
          stats(data1997).intersection(
          stats(temps2017).intersection(
          stats(data1977).intersection(
          stats(data1967).intersection(
          stats(data1957).intersection(
          stats(data1947).intersection(
          stats(data1937).intersection(
          stats(data1927).intersection(
          stats(data1917).intersection(
          stats(data1907).intersection(
          stats(temps1987).intersection(sameStat)))))))))))).collect.toSet

        val yearAvesA = Array((1897, problem3A(temps1897)),
          (1907, problem3A(data1907)),
          (1917, problem3A(data1917)),
          (1927, problem3A(data1927)),
          (1937, problem3A(data1937)),
          (1947, problem3A(data1947)),
          (1957, problem3A(data1957)),
          (1967, problem3A(data1967)),
          (1977, problem3A(data1977)),
          (1987, problem3A(temps1987)),
          (1997, problem3A(data1997)),
          (2007, problem3A(data2007)),
          (2016, problem3A(temps2016)),
          (2017, problem3A(temps2017)))
        val cScatterPlot = Plot.scatterPlot(yearAvesA.map{case (year, a) => year}, yearAvesA.map{case (year, a) => a}, "Average Temperature By Year", "Year", "Temp", 4.0)
        FXRenderer(cScatterPlot)


        //D
        val yearAvesB = Array((1897, problem3B(temps1897, statList)),
          (1907, problem3B(data1907, statList)),
          (1917, problem3B(data1917, statList)),
          (1927, problem3B(data1927, statList)),
          (1937, problem3B(data1937, statList)),
          (1947, problem3B(data1947, statList)),
          (1957, problem3B(data1957, statList)),
          (1967, problem3B(data1967, statList)),
          (1977, problem3B(data1977, statList)),
          (1987, problem3B(temps1987, statList)),
          (1997, problem3B(data1997, statList)),
          (2007, problem3B(data2007, statList)),
          (2016, problem3B(temps2016, statList)),
          (2017, problem3B(temps2017, statList)))
        val dScatterPlot = Plot.scatterPlot(yearAvesB.map{case (year, a) => year}, yearAvesB.map{case (year,a) => a}, "Average Temperature By Year", "Year", "Temp", 4.0)
        FXRenderer(dScatterPlot)




        def stats(rdd: RDD[DataDay]): RDD[String] = {
          rdd.filter(a => a.element == "TMAX" || a.element == "TMIN").map(_.station).distinct
        }



        def problem3A(rdd : RDD[DataDay]) : Double = {
            val totalAve = rdd.aggregate((0.0, 0.0))({ case ((sum, count), a) =>  (sum + a.value, count +1)}, ((x,y) => (x._1 + y._1, x._2 + y._2)))
      //      (totalAve._1/totalAve._2)
      //    }

        def problem3B(rdd: RDD[DataDay], statList: Set[String]): Double = {
            val filteredRdd = rdd.filter(a => statList.contains(a.station))
            val totalAve = filteredRdd.aggregate((0.0, 0.0))({ case ((sum, count), a) => (sum + a.value, count +1)}, ((x,y) => (x._1 + y._1, x._2 + y._2)))
            (totalAve._1/totalAve._2)
        }


*/

        def standardDev(rdd : RDD[DataDay]) : Double = {
          val tempCounts = rdd.aggregate(0.0, 0.0)/*(sum, count)*/(
            {case((sum, count), b ) => (sum + b.value, count + 1)},
            {case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)})
          val ave = tempCounts._1 / tempCounts._2
          val averageSum = rdd.aggregate(0.0)((a,b) => (a + Math.pow(b.value - ave, 2)), (x, y) => x+y)
          Math.sqrt(averageSum/tempCounts._2)
        }

        def averageTemps(rdd: RDD[DataDay], date: Boolean) : RDD[DataDay] = {
          if(date) {
            val groupedRdd = rdd.map(a => (a.station, a.date) -> a)
            val dailySum = groupedRdd.aggregateByKey((0.0))({ case (diff, a) => /*if(diff == 0.0) a.value else*/ (diff + a.value)}, ((x,y) => x+y))
            dailySum.map{case (stationD, data) => DataDay(stationD._1, stationD._2, "TAVE", data/2)}
          } else {
            val groupedRdd = rdd.filter(a => a.element == "TMAX").map(a => (a.station) -> a)
            val dailySum = groupedRdd.aggregateByKey((0.0, 0.0))({ case ((sum, count), a) => /*if(diff == 0.0) a.value else*/ (sum + a.value, count+1)}, ((x,y) => (x._1 + y._1, x._2 + y._2)))
            dailySum.map{case (stationD, (sum, count)) => DataDay(stationD, 0, "TAVE", sum/count)}
          }
        }

}
