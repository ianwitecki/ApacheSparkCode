import scalafx.application.JFXApp
import org.apache.spark._


object HelloSpark extends JFXApp {
        val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val lines2017 = sc.textFile("/users/mlewis/CSCI3395-F17/data/ghcn-daily/2017.csv")
        println(lines2017.take(5));

}
