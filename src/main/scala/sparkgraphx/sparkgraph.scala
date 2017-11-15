
package sparkgraphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
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




object MedSet extends JFXApp {

 Logger.getLogger("org").setLevel(Level.OFF)


 val conf = new SparkConf().setAppName("Graph Application").setMaster("local[*]")
 val sc = new SparkContext(conf)
 sc.setLogLevel("WARN")

	val cits = descriptorReader("/data/BigData/Medline/medsamp2016a.xml")
	//var citationNodes = cits

	/*for (i <- 'b' to 'c') {
		citationNodes = citationNodes ++ descriptorReader("/data/BigData/Medline/medsamp2016" + i + ".xml") 
	}*/

	val citationNodes = cits ++ descriptorReader("/data/BigData/Medline/medsamp2016" + "b" + ".xml") ++ descriptorReader("/data/BigData/Medline/medsamp2016" + "c" + ".xml") 
	//IN CLASS CODE

	val majorKeys = citationNodes.flatten.filter(n => (n \ "@MajorTopicYN").toString == "Y").map(_.text)
	val citationSeq = citationNodes.flatten.map(_.text)

	//Problem 1
	println("Number of Descriptors")
	val count = citationSeq.distinct.count(_ => true)
	println(count)
/*
	//Problem 2
	println("All Descriptors")
	val citRDD = sc.parallelize(citationSeq)
	val countMap = citRDD.countByValue.toSeq
	sc.parallelize(countMap).sortBy(_._2, ascending=false).take(10) foreach println

	//Problem 3
	println("Major Topics")
	val majorKeyRDD = sc.parallelize(majorKeys)
	val countMap1 = majorKeyRDD.countByValue.toSeq
	sc.parallelize(countMap1).sortBy(_._2, ascending=false).take(10) foreach println

	//Problem 4
	println("Math: Number of Pairs")
	println((count * (count - 1)) / 2)
*/

	//Problem 5
	val citPairSeq = sc.parallelize(citationNodes.map(seq => seq.map(node => node.text)))
	.flatMap{seq =>
		seq.combinations(2)
	}
	citPairSeq.take(10) foreach println
	println(citPairSeq.distinct.count())


	/*println("Number of Pairs")
	val citEdges = citRDD.flatMap(citation => citation.combinations(2))
	println(citEdges.count)*/

	//Problem 6
	/* The graph of pair terms should not be directed becuase there will never be case where a descriptor has an edge to another descriptor, but there is no edge from that the destination descriptor back. This implies that our graph is only interested in how related different descriptors are to each other*/

	// OUT OF CLASS CODE 
	
	//ALL DESCRIPTORS
	val regNodeIndexPairs = citationSeq.distinct.zipWithIndex.map { case (n, i) => i.toLong -> n }

	val indexMap = regNodeIndexPairs.map { case (i, n) => n -> i }.toMap


	val regEdgePairs = citPairSeq.distinct.flatMap{ list => 
		Seq( Edge(indexMap(list(0)), indexMap(list(1)), ()), 
			 Edge(indexMap(list(1)), indexMap(list(0)), ()))
	}


	val	regNodeRDD = sc.parallelize(regNodeIndexPairs)
	val regGraph = Graph(regNodeRDD, regEdgePairs)

	println(regGraph.numVertices)

	// Connected Components
	println("Connected Components \n")
//	println(regGraph.connectedComponents().vertices.map(_._2).distinct.count())

	//Top Words By Page Rank
	println("Page Rank \n")
//	regGraph.pageRank(0.01).vertices.sortBy(_._2, ascending = false).take(10).map(a => (a._2, regNodeIndexPairs((a._1).toInt))) foreach println

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
/*
	val pregnancySP = ShortestPaths.run(regGraph, Seq(indexMap("Esophagus")))
    println(pregnancySP.vertices.filter(_._1==indexMap("Pregnancy")).first)


	val arterySP = ShortestPaths.run(regGraph, Seq(indexMap("Femoral Artery")))
    println(arterySP.vertices.filter(_._1==indexMap("Electroencephalography")).first)

	val taxesSP = ShortestPaths.run(regGraph, Seq(indexMap("Taxes")))
    println(taxesSP.vertices.filter(_._1==indexMap("Guinea Pigs")).first)
*/
	//MAJOR DESCRIPTORS

def descriptorReader(path: String): Seq[NodeSeq] = {
	val parsed = xml.XML.loadFile(path)
	val cit = (parsed \ "MedlineCitation").map(x => (x \\ "DescriptorName"))
	return cit
}

sc.stop

}
