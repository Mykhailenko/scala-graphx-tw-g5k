package util

object s {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  val x = 2 + 8                                   //> x  : Int = 10
   
  import scala.io.Source

import spray.json._
import DefaultJsonProtocol._


  val source = scala.io.Source.fromFile("/user/hmykhail/home/phd/scalap/scala-graphx-tw-g5k/ola.json").mkString
                                                  //> source  : String = "{
                                                  //| 	"master" : "huyaster",
                                                  //| 	"time" : "1232454"
                                                  //| }
                                                  //| "
  val jsonAst = source.parseJson                  //> jsonAst  : spray.json.JsValue = {"master":"huyaster","time":"1232454"}
  val map = jsonAst.convertTo[Map[String, String]]//> map  : Map[String,String] = Map(master -> huyaster, time -> 1232454)
   
}