package util

import org.apache.spark.SparkContext
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File
import java.util.Random

case class JsonLogger(sparkContex: SparkContext, path: String = "./")(body: JsonLogger => Unit) extends Logger {

  var mainObj: Map[String, String] = Map()

  def log(key: String, value: String) {
    mainObj += (key -> value)
  }

  def presave {
    mainObj += ("master" -> sparkContex.master)
  }

  def fileName : String = (System.nanoTime() * (new Random()).nextLong() % 1000).toString + ".json" 
  
  def save {
    val out = new PrintWriter(new FileWriter(new File(path + fileName)));
    out println "{\n"
    val lst : List[(String, String)] = mainObj.toList
    for(i <- 0 until lst.length -1; (key, value) = lst(i)){
      out println "\t\"" + key + "\" : \"" + value + "\",\n"
    }
    val (key, value) = lst(lst.length - 1) 
    out println "\t\"" + key + "\" : \"" + value + "\"\n}"
    out close
  }

  body(this)
  presave
  save

}