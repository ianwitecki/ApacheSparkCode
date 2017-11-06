package sparkml
import scala.io.Source
import java.io._


object ParseNetflix {
    def main(args: Array[String]) {
    val rat = scala.io.Source.fromFile("/data/BigData/Netflix/combined_data_1.txt").getLines
    val rat2 = scala.io.Source.fromFile("/data/BigData/Netflix/combined_data_2.txt").getLines

  var movieid = "";
  val bw = new BufferedWriter(new FileWriter(new File("/users/iwitecki/BigData/Data/parsed_ratings_1.txt")))
  println("Start Parse")
  rat.foreach {line => 
        if (line.contains(":")) {
                movieid = line.split(":")(0)
                print(movieid + "\n")
        } else {
                bw.write(line + "," + movieid + "\n")
        }
  }

  var count = 0;
  rat2.foreach {line => 
        if (line.contains(":")) {
                movieid = line.split(":")(0)
                print(movieid + "\n")
        } else {
                if (count < 500) {
                  count = count + 1
                  bw.write(line + "," + movieid + "\n")
                }
        }
  }


  bw.close()
}
}
