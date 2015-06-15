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
import java.util.Random
import com.google.common.collect.Sets
import scala.collection.JavaConversions

object Degree {

  def main(args: Array[String]) {

    val numberOfArguments = 2
    require(args.length == numberOfArguments, s"""Wrong argument number. Should be $numberOfArguments . 
                                                 |Usage: <path_to_grpah> <filename_with_result>""".stripMargin)

    var graph: Graph[Int, Int] = null
    val nameOfGraph = args(0).substring(args(0).lastIndexOf("/") + 1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))
    
    graph = GraphLoader.edgeListFile(sc, args(0), false)
    
    val result = graph.degrees
      .collect
      .groupBy(_._2)
      .map(a => (a._1, a._2.length))
      .toArray
      .sortWith(_._1 < _._1)

    val out = new PrintWriter(new FileWriter(new File(args(1))));
    
    for (line <- result) {
      out.println(line._1 + "\t" + line._2)
    }
    
    out close
    
    
  }
}