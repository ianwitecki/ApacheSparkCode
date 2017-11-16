
package sparkgraphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths
import scala.xml._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx.lib.ShortestPaths
import scalafx.application.JFXApp
import scalafx.Includes._
import scalafx.scene.Scene
import scalafx.scene.chart.ScatterChart
import scalafx.scene.chart.NumberAxis
import scalafx.scene.chart.XYChart
import scalafx.scene.layout.TilePane
import scalafx.collections.ObservableBuffer
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer




object MedSet extends App {

 Logger.getLogger("org").setLevel(Level.OFF)


 val conf = new SparkConf().setAppName("Graph App").setMaster("local[*]")
 val sc = new SparkContext(conf)

//	val cits = descriptorReader("/data/BigData/Medline/medsamp2016a.xml")
//	var citationNodes = cits

//	for (i <- 'b' to 'h') {
//		citationNodes = citationNodes ++ descriptorReader("/data/BigData/Medline/medsamp2016" + i + ".xml") 
//	}

	println("Parse \n")

	val citationNodes =  descriptorReader("/data/BigData/Medline/medsamp2016a.xml") ++ descriptorReader("/data/BigData/Medline/medsamp2016" + "b" + ".xml") ++ descriptorReader("/data/BigData/Medline/medsamp2016" + "c" + ".xml")	++ descriptorReader("/data/BigData/Medline/medsamp2016" + "d" + ".xml")	++ descriptorReader("/data/BigData/Medline/medsamp2016" + "e" + ".xml")	++ descriptorReader("/data/BigData/Medline/medsamp2016" + "f" + ".xml")	++ descriptorReader("/data/BigData/Medline/medsamp2016" + "g" + ".xml")	++ descriptorReader("/data/BigData/Medline/medsamp2016" + "h" + ".xml") 
	//IN CLASS CODE

	println("Code Parsed \n")


	//val majorKeys = citationNodes.flatten.filter(n => (n \ "@MajorTopicYN").toString == "Y").map(_.text).distinct
	val citationSeq = citationNodes.map(_._1).distinct

	citationSeq.take(5) foreach println

	//Problem 1
	println("Number of Descriptors")
	val count = citationSeq.count(_ => true)
	println(count)

/*	//Problem 2
	println("\n All Descriptors")
	val citRDD = sc.parallelize(citationSeq)
	val countMap = citRDD.countByValue.toSeq
	sc.parallelize(countMap).sortBy(_._2, ascending=false).take(10) foreach println
*/
	//Problem 3
	/*println("\n Major Topics")
	val majorKeyRDD = sc.parallelize(majorKeys)
	val countMap1 = majorKeyRDD.countByValue.toSeq
	sc.parallelize(countMap1).sortBy(_._2, ascending=false).take(10) foreach println
*/
	//Problem 4
	println("\n Math: Number of Pairs")
	println((count * (count - 1)) / 2)


	//Problem 5
	println("\n Number of Pairs")
	/*val pairSeq = sc.parallelize(citationNodes.map(seq => seq.map(node => node._1)))
	.flatMap{seq =>
		seq.combinations(2)
	}
	println(pairSeq.distinct.count())*/


	// OUT OF CLASS CODE 
/*
	def graphAssignment(allNodes: Boolean):Unit = {

	
	val regNodeIndexPairs = assignNodes(allNodes)
	
	val citPairSeq = assignPairs(allNodes) 

	

	val indexMap = regNodeIndexPairs.map { case (i, n) => n -> i }.toMap


	val regEdgePairs = citPairSeq.distinct.flatMap{ list => 
		Seq( Edge(indexMap(list(0)), indexMap(list(1)), ()), 
			 Edge(indexMap(list(1)), indexMap(list(0)), ()))
	}


	val	regNodeRDD = sc.parallelize(regNodeIndexPairs)
	val regGraph = Graph(regNodeRDD, regEdgePairs)

	// Connected Components
	println("Connected Components \n")
	println(regGraph.connectedComponents().vertices.map(_._2).distinct.count())

	//Top Words By Page Rank
	println("Page Rank \n")
	regGraph.pageRank(0.01).vertices.sortBy(_._2, ascending = false).take(10).map(a => (a._2, regNodeIndexPairs((a._1).toInt))) foreach println

	//  Degree Distribution
	println("Degree Distribution \n")
	val degreeDist = regGraph.degrees.sortBy(_._1)

//	val bins = degreeDist.map(a => (a._1).toDouble).collect()
	println(count)
	println(degreeDist.count)
	degreeDist.take(5) foreach println
	println()
	degreeDist.sortBy(_._1, false).take(5) foreach println

	val bins = (0.0 to count.toDouble by 10.0).toArray
	val y = degreeDist.map(_._1.toDouble).collect
    val hist = degreeDist.map(_._2.toDouble).histogram(y, true)
    val plot = Plot.histogramPlot(bins, hist, RedARGB, true, "Degree Distribution of All Descriptors", "Vertex", "Degree Distribution")
     FXRenderer(plot)


	//	Shortest Path

	val pregnancySP = ShortestPaths.run(regGraph, Seq(indexMap("Esophagus")))
    println(pregnancySP.vertices.filter(_._1==indexMap("Pregnancy")).first)


	val arterySP = ShortestPaths.run(regGraph, Seq(indexMap("Femoral Artery")))
    println(arterySP.vertices.filter(_._1==indexMap("Electroencephalography")).first)

	val taxesSP = ShortestPaths.run(regGraph, Seq(indexMap("Taxes")))
    println(taxesSP.vertices.filter(_._1==indexMap("Guinea Pigs")).first)

	}

	def assignNodes(all: Boolean) : Seq[(Long, String)] = {
		if (all) {
			citationSeq.distinct.zipWithIndex.map { case (n, i) => i.toLong -> n }
		} else {
			majorKeys.distinct.zipWithIndex.map { case (n, i) => i.toLong -> n }
		}
	}

	def assignPairs(all: Boolean) : RDD[Seq[String]] = {

	if (all) {
		sc.parallelize(citationNodes.map(seq => seq.map(node => node.text)))
		.flatMap{seq =>
		seq.combinations(2)
		}

	} else {
		 sc.parallelize(citationNodes.map{seq => 
			seq.filter{n => 
				(n \ "@MajorTopicYN").toString == "Y"
				}
				.map(node => node.text)
			}
		)
		.flatMap{seq =>
		seq.combinations(2)
		}
	}
	}
*/
def descriptorReader(path: String): Seq[(String, String)] = {
	val parsed = xml.XML.loadFile(path)
/*	val cit = (parsed \ "MedlineCitation").map(x => (x \\ "DescriptorName")).map{seq => 
	            seq.map{n => 
	                 (n.text, (n \ "@MajorTopicYN").toString)
	                }
             }*/
	val cit = (parsed \ "MedlineCitation" \\ "DescriptorName").map{ n => 
	                 (n.text, (n \ "@MajorTopicYN").toString)
	                }
	cit

}

sc.stop

}
