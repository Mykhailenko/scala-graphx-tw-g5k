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
import it.unipd.dei.graphx.diameter.DiameterApproximation

object Diameter {

  def main(args: Array[String]) {

    val numberOfArguments = 2
    require(args.length == numberOfArguments, s"""Wrong argument number. Should be $numberOfArguments .
                                                 |Usage: <path_to_grpah> <filename_with_result>""".stripMargin)


    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))


    val g = GraphLoader.edgeListFile(sc, args(0), false).mapEdges(x => 0.0 )



    val out = new PrintWriter(new FileWriter(new File(args(1))));

//    DiameterApproximation.run(graph)

    out close

  }
}
