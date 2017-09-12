import scalafx.application.JFXApp
import scalafx.Includes._
import scalafx.scene.Scene
import scalafx.scene.chart.LineChart
import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import scalafx.collections.ObservableBuffer
import scala.collection.immutable.Set
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level


case class DataDay(station: String, date: Int, element: String, value: Int);
case class Station(station: String, lat: Double, long: Double, elev: Double, state: String, name: String);


object InClass1 extends JFXApp {
        
        
        Logger.getLogger("org").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
        val data2017 = lines2017.map { line => 
                                                val d = line.split(",")
                                                DataDay(d(0), d(1).toInt, d(2), d(3).toInt)
                                     }
        val stationLines = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/ghcnd-stations.txt")
        val stations = stationLines.map { line =>
                                                Station(line.substring(0,11), line.substring(13,20).toDouble, line.substring(22,30).toDouble, line.substring(32,37).toDouble, line.substring(38,40), line.substring(41, 71))
        } 
                                                
        
        //Print Total Number of Stations In Texas
        val texasStations = stations.filter(_.state == "TX")
        println("Texas Stations: " + texasStations.count())
        

        
        //Print Stations that Reported Data
        val texasCodes = texasStations.map(a => a.station).collect.toArray.toSet
        val dailyTexas = data2017.filter(day => texasCodes.contains(day.station))
        //val dataStations = data2017.map(a => a.station)
        //val stationIntersect = texasCodes.intersection(dataStations)
        println("Texas Stations Reporting Data: " + dailyTexas.map(a => a.station).distinct().count())

      /*  
        //Highest Reported Temperature
        val hottestDay = data2017.filter(_.element == "TMAX").reduce((a, b) => if (b.value > a.value) b else a)
        val location = stations.filter(a => a.station == hottestDay.station).collect
        println("Hottest Location Reported: " + location(0).name)
        println("Highest Temperature: " + hottestDay.value)
       
         
        //Stations Reporting No Data In 2017
        val stationCodes = stations.map(a => a.station)
        val dataStations = data2017.map(a => a.station)
        val nonReportingStations = stationCodes.subtract(dataStations)
        println("Stations Not Reporting Data: " + nonReportingStations.count())
        
        //Max Rainfall for Texas Stations 
        val maxRainfall = dailyTexas.reduce{ (a,b) => 
              if (b.element == "PRCP" && b.value > a.value) b else a
        }
       println("Texas Maximum Rainfall: " + maxRainfall.value + " on " + maxRainfall.date + " at station " + maxRainfall.station) 
  
        //Max Rainfall for Stations in India
        val indianRainfall =  data2017.filter(_.station.substring(0,2) == "IN").reduce { (a,b) => 
                if (b.element == "PRCP" && b.value> a.value) b else a}
        println("Indian Maximum Rainfall: " + indianRainfall.value + " on " + indianRainfall.date + " at station " + indianRainfall.station) 
*/
        //Weather Stations Associated with San Antonio
/*
        val sanAntonStations = texasStations.filter(_.name.contains("SAN ANTONIO")).map(a => a.station).collect.toArray.toSet
        println("San Antonio Stations: " + sanAntonStations.size)
        
        //San Antonio Stations Reporting Temperature
        val dataSanAntonio = dailyTexas.filter(day => sanAntonStations.contains(day.station))
        println("San Antonio Stations Reporting Data: " + dataSanAntonio.filter(_.element =="TMAX").map(a => a.station).distinct().count())
       
        //Largest Daily Increase in High Temp in San Antonio 
        val tempDifferences = dataSanAntonio.filter(_.element == "TMAX")
                                .groupBy(_.station)
                                .map{case (s, data) => data.toArray.sortBy(_.date)
                                .aggregate(0,1000,0)( (a,b) => if (b.value - a._2 > a._1) (b.value - a._2, b.value, b.date) else (a._1, b.value, a._3), (x, y) => if (x._1 > y._1) x else y)
                                }
        val tempRange = tempDifferences.reduce((a,b) => if (a._1 > b._1) a else b)
        println("The greatest increase in temperature was " + tempRange._1 + " on " + (tempRange._3 -1) + " to " + tempRange._3) 

        //Correlation Coefficients
        val weatherTuples = dataSanAntonio.filter(data => data.element == "TMAX" || data.element == "PRCP")
                                .groupBy(a => (a.station, a.date))
                                .filter{ case(station,data) =>  
                                                        data.size > 1
                                        }
                                .map{ case(stDay, data) => data.aggregate(0,0)((a,b) => if (b.element == "PRCP") (a._1, b.value) else (b.value, a._2), (x, y) => x)
                                        }
                                
        val (x, y, x2, y2, xy, acc) = weatherTuples.aggregate(0.0,0.0,0.0,0.0,0.0,0.0)((a,b) => (a._1 + b._1, a._2 +b._2, a._3 + (b._1*b._1), a._4 + (b._2*b._2), a._5 + (b._1*b._2), a._6 + 1), (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))        
        

        val coral = ((acc * xy) - (x * y))/Math.sqrt(((acc * x2) - (x * x))*((acc * y2) - (y * y)))
        println(coral)
*/
        //That Fucking Graph 
        
        //Sugarloaf Mtn USS0046M04S
        
       

        def chart (stationCode: String): XYChart.Series[Number ,Number] = {
                
                val stationTemps = data2017.filter(a => a.element == "TMAX" && a.station == stationCode).collect
                val tempTuples = stationTemps.map(a => XYChart.Data[Number, Number](datify(a.date.toString), a.value/10)).toSeq
                XYChart.Series[Number, Number](stationCode, ObservableBuffer(tempTuples))
        }

        def datify (date: String): Int = {
                val month = date.substring(4,6).toInt
                val day = date.substring(6,8).toInt
                if (month > 1) {
                        ((month-1)*30) + day
                } else {
                       day
                }  
        }

        stage = new JFXApp.PrimaryStage {
                title .value = "Temperature Chart"
                width = 800
                height = 650
                scene = new Scene {
                        val dateAxis = new NumberAxis(0, 365, 2)
                        dateAxis.label = "Day of the Year"
                        val tempAxis = new NumberAxis(-20, 45, 1)
                        tempAxis.label = "Temperature"
                        val chart1Data = data2017.filter(a => a.element == "TMAX" && a.station == "USW00012917").collect.map(a => XYChart.Data[Number, Number](datify(a.date.toString), a.value/10)).toSeq 
                        val chart1 = XYChart.Series[Number,Number]("USW00012917", ObservableBuffer(chart1Data))
                        val chart2Data = data2017.filter(a => a.element == "TMAX" && a.station == "IN017111200").collect.map(a => XYChart.Data[Number, Number](datify(a.date.toString), a.value/10)).toSeq 
                        val chart2 = XYChart.Series[Number,Number]("IN017111200", ObservableBuffer(chart2Data))

                        

                        val sc = new LineChart(dateAxis, tempAxis, ObservableBuffer(chart1, chart2))
                        sc.setTitle("Temperature by the Day of Year")
                        sc.legendVisible = false
                        sc.minHeight_= 600
                        sc.minWidth_ = 750
                        content = sc
                }

        }
}

                



