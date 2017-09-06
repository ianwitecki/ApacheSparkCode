import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.scene.Scene
import scalafx.scene.paint.Color._
import scalafx.scene.shape.Rectangle
import scalafx.scene.chart.ScatterChart
import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import scalafx.collections.ObservableBuffer


object TempChart extends JFXApp {
        val source = io.Source.fromFile("/users/mlewis/CSCI1320-S17/SanAntonioTemps.csv")
        val lines = source.getLines.drop(2)
        val tempData = lines.map { line =>
        val p = line.split(",")
        TempData(p(0).toInt, p(1).toInt, p(2).toInt, p(4).toInt, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
}.toArray
        var tempByDay = tempData.map(data => (data.doy, data.tmax))
        ObservableBuffer(tempByDay.map(data => XYChart.Data[Number, Number](data._1, data._2)).toSeq)

        source.close

        stage = new JFXApp.PrimaryStage {
                title .value = "Temperature Chart"
                width = 600
                height = 450
                scene = new Scene {
                        val dateAxis = new NumberAxis(0, 365, 10)
                        dateAxis.label = "Day of the Year"
                        val tempAxis = new NumberAxis(20, 110, 10)
                        tempAxis.label = "Temperature"
                        val sc = new ScatterChart(dateAxis, tempAxis, ObservableBuffer(XYChart.Series[Number, Number](ObservableBuffer(tempByDay.map(data => XYChart.Data[Number, Number](data._1, data._2)).toSeq))))
                        sc.setTitle("Temperature by the Day of Year") 
                        sc.legendVisible = false
                        content = sc
                }                

        }
}
