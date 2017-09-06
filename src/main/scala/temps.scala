
case class TempData(day: Int, doy: Int, month:Int, year: Int, precip: Double, tave: Double, tmax: Double, tmin: Double)

object TempAnalysis {

def main(args:Array[String]): Unit = {

val source = io.Source.fromFile("/users/mlewis/CSCI1320-S17/SanAntonioTemps.csv")
val lines = source.getLines.drop(2)
val tempData = lines.map { line => 
        val p = line.split(",")
        TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
}.toArray

//greatest temps
println(tempData.reduceLeft((a, b) => if (a.tmax > b.tmax) a else b))
println(tempData.maxBy(_.tmax))

//most rain
println(tempData.maxBy(_.precip))

//days with more than 1 inch
println(tempData.count(_.precip > 1) + "/" + tempData.length)

//average temperature for the rainy days
var rainyTuple = tempData.foldLeft((0.0, 0)){(a, b) => 
                        if (b.precip >= 1) 
                                (a._1 +b.tmax, a._2+1 )
                        else a}
println(rainyTuple._1/rainyTuple._2)

//average high temperature by month
println("AVERAGE TEMPS")
var monthMap = tempData.groupBy(_.month)
var monthlyAves = monthMap.map { case(month, tds) =>
                val aveTemp = tds.foldLeft(0.0)((a,b) => a+b.tmax)/tds.length
                (month, aveTemp)
             }
monthlyAves.toSeq.sortBy(_._1) foreach println

println("AVERAGE RAIN") 
//average precipitation per month 
var rainyAves = monthMap.map { case(month, tds) => 
                val avePrecip = tds.foldLeft(0.0)((a,b) => a+b.precip)/tds.length
                (month, avePrecip)
             }
rainyAves.toSeq.sortBy(_._1) foreach println


println("MEDIAN PRECIPITATION") 
//Average Median temps by month
var medianAves = monthMap.map { case(month, tds) => 
                val sortedPrecip = tds.sortBy(_.precip)
                (month, sortedPrecip(tds.length/2))
             }
medianAves.toSeq.sortBy(_._1) foreach println

println("TEMPS BY DAY")
//Temperature to Day
var tempByDay = tempData.map(data => (data.doy, data.tmax))
tempByDay.take(5) foreach println



source.close



}
}
