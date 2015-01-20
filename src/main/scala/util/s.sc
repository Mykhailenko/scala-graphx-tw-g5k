package util

object s {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val x = 2 + 8                                   //> x  : Int = 10
   
  import scala.io.Source

import spray.json._
import DefaultJsonProtocol._


  
  2 + 2                                           //> res0: Int(4) = 4
  
 import util.CSVChartData
 val c = new CSVChartData[Double]("astroph", "vertexCut")
                                                  //> c  : util.CSVChartData[Double] = util.CSVChartData@3a25b987
 c.addPoints("RandomHuyandom", Array[(Int, Double)]((1, 2.0),(2, 10.0)))
 c.addPoints("RandomHuyand2om", Array[(Int, Double)]((1, 2.2),(2, 104.0)))
 c.numberOfPartitioners                           //> res1: Int = 2
 c.numberOfExperiments                            //> res2: Int = 2
 
 c.csv                                            //> res3: String = "astroph vertexCut ,  , 
                                                  //| Number of partitions , RandomHuyandom , RandomHuyand2om
                                                  //| 1 , 2.0 , 2.2
                                                  //| 2 , 10.0 , 104.0
                                                  //| "
}