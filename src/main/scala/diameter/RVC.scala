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

object RVC {

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

    val inD = graph.inDegrees
    			   .collect
    			   .groupBy(_._2)
    			   .map(a => (a._1, a._2.length))
    			   .toArray
    			   .sortWith(_._1 < _._1)
    			   
    
    			   
      
    val T = result.map(x => x._1 * x._2).reduce(_ + _)
    assert(T == graph.numEdges * 2, "Thug phd 0; it is " + T + ", but should be " + graph.numEdges * 2)
    
      
    val out = new PrintWriter(new FileWriter(new File(args(1))));
    
    

    out println("#N\tRVC\tCRVC\tEP1D\tEP2D")
    out println ("1\t0\t0\t0\t0")
    for (N <- 2 to 50) {
      var rvc : Long = 0
      var crvc : Long = 0
      var ep2d : Long = 0
      
      for (line <- result) {
        val degree = line._1
        val numberOfVertices = line._2
        var P = (1 / math.pow(N, degree - 1))
        assert(P <= 1.0, "Probability is greater than 1: " + P)    
        rvc += (numberOfVertices * P).toLong
        P = (1 / math.pow(N, degree/2 - 1))
        assert(P <= 1.0, "Probability is greater than 1: " + P)
        crvc += (numberOfVertices * P).toLong
        val M = math.ceil(math.sqrt(N))
        val C = N / (M * M)
        P = 1 / math.pow( 2 * M - 1 , degree - 1)
        ep2d += (numberOfVertices * P).toLong
      }
      
      var ep1d : Long = 0
      for(line <- inD){
        val inDegree = line._1
        val numberOfVertices = line._2
        val P = (1 / math.pow(N, inDegree))
        ep1d += (numberOfVertices * P).toLong
      }
      
      
      
      out.println(N + "\t" + (graph.numVertices -  rvc)+ "\t" + (graph.numVertices -  crvc) + "\t" + (graph.numVertices - ep1d) + "\t" + (graph.numVertices - ep2d))
    }
    
    
    out close

  }

}