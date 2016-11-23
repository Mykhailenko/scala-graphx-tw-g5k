package diameter

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.commons.lang3.tuple.Triple

object BiggestComponent {
  def main(args: Array[String]) {

    val pathToGrpah = args(0)
    val minimumNumberOfNeighbours = args(1).toInt
    val filenameWithResult = args(2)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, pathToGrpah, false,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

    val filtered = graph.collectNeighborIds(EdgeDirection.Out).filter(x => x._2.length > minimumNumberOfNeighbours)
    if (filtered.count == 0) {
      println("filtered.count == 0")
      System.exit(1)
    }

    val sourceId: VertexId = filtered.first._1

    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0 else 1)
    
    val sssp = initialGraph.pregel(1, activeDirection = EdgeDirection.Out)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {
        if (triplet.srcAttr == 0 && triplet.dstAttr != 0) {
          Iterator((triplet.dstId, 0))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
      )
    val edges = sssp.triplets.filter(triplet => triplet.srcAttr == 0 && triplet.dstAttr == 0).
      map(triplet => triplet.srcId + " " + triplet.dstId)

    edges.coalesce(1).saveAsTextFile(filenameWithResult)
  }
}