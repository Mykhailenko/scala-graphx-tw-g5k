package util

import util.MetricsBuilder._
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import util.EventLogParser._

object TimeDecomposer {
  def main(args: Array[String]) {
    val path = new File(args(0))
	val data = jsonWorker(path, ".*twitter.*", ".*", ".*", ".*").view.map(x => {
	  if(x.get("applicationTime.time").isDefined &&
	      x.get("GraphLoading.time").isDefined &&
	      x.get("Partitioning.time").isDefined &&
	      x.get("ResultSaving.time").isDefined &&
	      x.get("AlgorithExecution.time").isDefined ){
	    
	  }
	    
	})
	
	
  }

}
