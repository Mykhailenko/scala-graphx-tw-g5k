package util

import scala.io.Source

import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object NetworkShuffle {

  def main(args: Array[String]) {

    var mainObj: Map[String, String] = Map()

    require(args.length == 2, """Should have two parameters: master url and path to result""".stripMargin)

    val master = args(0)
    val path = args(1)
    val html = Source.fromURL(master)
    val content = html.mkString
    val preId = "<a href=\"app?appId="

    val beginIndex = content.indexOf(preId) + preId.length()

    val executorId = readFromIndexUntilQuotes(content, beginIndex)

    println("executorId = " + executorId)
    
    val newurl = master + "/history/" + executorId + "/executors/"
    
    println("newurl = " + newurl)

    val historyHtml = Source.fromURL(newurl)

    val preInOut = "<td sorttable_customkey=\""
    
    val contentHistory = historyHtml.mkString
      
    val result = findAllOccurencies(contentHistory, preInOut)
    println("result.length = " + result.length)
//    for(r <- result) {
//      println(r)
//    } 
    val resIn = result.drop(4).grouped(6).map(_.head.toInt).toList
    for(x <- resIn){
      println(x)
    }

    val input = resIn.reduce(_ + _)
    
    val resOut = result.drop(5).grouped(6).map(_.head.toInt).toList
    for(x <- resOut){
      println(x)
    }
    val output = resOut.reduce(_ + _)
    
    mainObj += ("input" -> input.toString)
    mainObj += ("output" -> output.toString)
    
    save(mainObj, path)
  }

  def save (mainObj: Map[String, String], path : String){

    val out = new PrintWriter(new FileWriter(new File(path)));
    out println "{"
    val lst: List[(String, String)] = mainObj.toList
    for (i <- 0 until lst.length - 1; (key, value) = lst(i)) {
      out println "\t\"" + key + "\" : \"" + value + "\","
    }
    val (key, value) = lst(lst.length - 1)
    out println "\t\"" + key + "\" : \"" + value + "\""
    out println "}"
    out close
  }
  
  def findAllOccurencies(content: String, preString: String): List[String] = {
    var result: List[String] = List()

    var beginIndex = content.indexOf(preString)
    
    while (beginIndex != -1) {
      result = result :+ readFromIndexUntilQuotes(content, beginIndex + preString.length())
      beginIndex = content.indexOf(preString, beginIndex + preString.length())
    }

    result
  }

  def readFromIndexUntilQuotes(content: String, beginIndex: Int): String = {
    var endIndex = beginIndex
    while (content.charAt(endIndex) != '\"') {
      endIndex = endIndex + 1
    }

    val res = content.substring(beginIndex, endIndex)
    res
  }

}