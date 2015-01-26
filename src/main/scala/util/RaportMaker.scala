package util

import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object RaportMaker {

  def main(args: Array[String]) {
  
    doit(args(0))
    
  }
  
  def doit(rootPath : String){
    
    val root = new File(rootPath)
    var str : String = ""
    val metrics = Array("vertexCut", "largestPartition", "NSTDEV", "communicationCost", "replicationFactor", "balance")
    for(graph <- root.listFiles() if graph.isDirectory())
      for(m <- metrics)
    	str += zaebato(graph, m)
    	
    val out = new PrintWriter(new FileWriter(root  + "/report"));
    out.print(str)
    out.close
    	
  }
  
  def zaebato(graph : File, metricName : String) : String = {
    val graphName = graph.getName()
    val data = new CSVChartData(graphName, metricName)
    for(p <- graph.listFiles() if p.isDirectory()){
      val partitionName = p.getName()
      var arr = Array[(Int, String)]()
      for(cas <- p.listFiles() if cas.isFile()){
        val partitionNumber = cas.getName().substring(0, cas.getName().length() - ".json".length).toInt
        val metricValue = new JsonReport(cas.getAbsolutePath()).get(metricName)
        arr = arr :+ (partitionNumber, metricValue)
      }
      arr = arr.sortWith(_._1 < _._1)
      data.addPoints(partitionName, arr)
    }
    
    data.csv
  } 
  
}