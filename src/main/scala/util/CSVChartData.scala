package util

class CSVChartData (val graphName : String, val metricName : String){

  var m: Array[(String, Array[(Int, String)])] = Array()

  def numberOfPartitioners: Int = m.size 

  def numberOfExperiments: Int = m.head._2.size 

  def addPoints(partitionerName: String, points: Array[(Int, String)]): Unit = m =  m :+ (partitionerName, points)

  def csv: String = {
    val arr: Array[Array[String]] = Array.fill[Array[String]](numberOfExperiments + 2)(Array.fill[String](numberOfPartitioners + 1)(""))

    arr(0)(0) = graphName + " " + metricName

    arr(1)(0) = "Number of partitions"
    for(j <- 0 until numberOfPartitioners){
      arr(1)(1 + j) = m(j)._1 
    }  
    
    for(i <- 0 until numberOfExperiments){
      arr(2 + i)(0) = m.head._2(i)._1.toString
      
      for(j <- 0 until numberOfPartitioners){
        arr(2 + i)(1 +j) = m(j)._2(i)._2
      }
    }
      
    
    var res = "";
    for (i <- 0 until arr.length) {
      for (j <- 0 until arr.head.length - 1) {
        res += arr(i)(j) + " , ";

      }
      res += arr(i)(arr.head.length - 1) + "\n"
    }

    res

  }
}