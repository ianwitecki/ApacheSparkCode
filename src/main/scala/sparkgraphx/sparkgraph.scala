
package sparkgraphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths
import scala.xml._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object MedSet extends App {

 Logger.getLogger("org").setLevel(Level.OFF)


 val conf = new SparkConf().setAppName("Graph Application").setMaster("local[*]")
 val sc = new SparkContext(conf)
 sc.setLogLevel("WARN")

	val cits = descriptorReader("/data/BigData/Medline/medsamp2016a.xml")
	var citationNodes = cits

	for (i <- 'b' to 'c') {
		citationNodes = citationNodes ++ descriptorReader("/data/BigData/Medline/medsamp2016" + i + ".xml") 
	}

	//val nodesRDD = sc.parallelize(nodes.foreach(_.toString).toArray)	

	val majorKeys = citationNodes.filter(n => (n \ "@MajorTopicYN").toString == "Y").map(_.text)
	val citationSeq = citationNodes.map(_.text)

	//Problem 1
	println("Number of Descriptors")
	val count = citationSeq.distinct.count(_ => true)
	println(count)

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


	//Problem 5
	println("Number of Pairs")
	val citEdges = citRDD.flatMap(citation => citation.combinations(2))
	println(citEdges.count)

	//Problem 6
	/* The graph of pair terms should not be directed becuase there will never be case where a descriptor has an edge to another descriptor, but there is no edge from that the destination descriptor back. This implies that our graph is only interested in how related different descriptors are to each other*/



def descriptorReader(path: String): NodeSeq = {
	val parsed = xml.XML.loadFile(path)
	val cit = (parsed \ "MedlineCitation" \\ "DescriptorName")
	return cit
}

sc.stop

}
