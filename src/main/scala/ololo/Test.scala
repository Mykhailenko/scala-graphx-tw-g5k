package ololo

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import sun.security.provider.certpath.Vertex

object Test {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Experiment avec la partiotioner")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setMaster("local[1]")
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7777L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph: Graph[(String, String), String] = Graph(users, relationships, defaultUser)
    println("START");
    val newGraph = graph.mapTriplets(triplet => 
      triplet.attr + "OLOLO"
    ).mapEdges(edge => edge.srcId)
    
    println(newGraph.edges.collect().mkString("\n"))
    println("FINAL");
  }
  def changer(triplet: EdgeTriplet[(String, String), String]): EdgeTriplet[(String, String), String] = {
    println("triplet from (" + triplet.srcAttr._1 + ", " + triplet.srcAttr._2 + ") to (" + triplet.dstAttr._1 + ", " + triplet.dstAttr._2 + ") with value " + triplet.attr)
    triplet
  }
}