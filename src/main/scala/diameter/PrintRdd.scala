package diameter

import util.JsonLogger0
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import sun.security.provider.certpath.Vertex
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.spark.storage.StorageLevel
import java.util.Random

object PrintRdd {

  def main(args: Array[String]) {

    val pathToGrpah = args(0)
    val filenameWithResult = args(1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val graph = GraphLoader.edgeListFile(sc, pathToGrpah, false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = 3)
    
    val out = new PrintWriter(new FileWriter(filenameWithResult, true));

    val ed = graph.edges.partitionsRDD.collect
    for(pair <- ed){
      out.println(pair._1)
      for(e <- pair._2){
        out.println(e.srcId + " " + e.dstId)
      }
    }	
    val ve = graph.vertices.partitionsRDD.collect
    for(ship <- ve){
      out.println("VALUES " )
//      for(v <- ship.values){
//        out.println(v)
//      }
      
      for(ss <- ship.iterator){
        out.println(ss._1)
      }
//      out.println("SHIP")
//      for(s <- ship.shipVertexIds){
//        out.println("SHIP TO " + s._1)
//        for(x <- s._2){
//          out.println(x)
//        }
//      }
    }
    
    val idAndESet = graph.edges.partitionsRDD.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*))).map(_._2)
    val idAndVSet = graph.vertices.partitionsRDD.map(x => x.iterator.toList.map(x => x._1).toSet)
    val zipped = idAndESet.zipPartitions(idAndVSet, true) ((x: Iterator[Set[VertexId]], y :Iterator[Set[VertexId]]) => {
      x.hasNext
      y.hasNext
      val xSet = x.next()
      val ySet = y.next()
      val local = xSet.intersect(ySet).size
      val all = xSet.size
      val remote = all - local
      Iterator(remote)
    })
//    
    out.println("Zipped")
    for (wtf <- zipped.collect()){
      
      out.println("WTF" + wtf)
    }
    
    out close
    
  }
}