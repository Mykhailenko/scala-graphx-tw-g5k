package diameter

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import sun.security.provider.certpath.Vertex
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.spark.graphx.impl.EdgePartition


object K {

  def main(args: Array[String]) {

    require(args.length == 2, "Wrong argument number. Should be 2. Usage: <path_to_grpah> <path_to_result> ")

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val file = new File(args(1));
    val out = new PrintWriter(new FileWriter(file));

    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, args(0), true, 24)

    out println("partiotioner default: " + calculReplicationFactor(graph) )
    out println("EdgePartition2D: " + calculReplicationFactor(graph.partitionBy(PartitionStrategy.EdgePartition2D)))
    out println("EdgePartition1D: " + calculReplicationFactor(graph.partitionBy(PartitionStrategy.EdgePartition1D)))
    out println("RandomVertexCut: " + calculReplicationFactor(graph.partitionBy(PartitionStrategy.RandomVertexCut)))
    out println("CanonicalRandomVertexCut: " + calculReplicationFactor(graph.partitionBy(PartitionStrategy.CanonicalRandomVertexCut)))

    
    
    out close

  }

  def calculReplicationFactor(G : Graph[Int, Int]) : String = {
    val t = G.edges.partitionsRDD
    val replic : Double = t.mapValues(
        (V) => (
            Set(V.srcIds: _*) ++ Set(V.dstIds: _*)
            ).size)
    
    
    .map(a => a._2).reduce((a, b) => a+b) 
    "" + replic / G.numVertices
  }
}
