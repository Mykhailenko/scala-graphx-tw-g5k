package util

import scala.io.Source
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Permutattor2 {
  def main(args: Array[String]) {
    require(args.length == 2, "Wrong argument number. Should be 1. Usage: <path_to_grpah> <path_to_permutatted>")

    var mapa = Map[Int, Int]()
    var rand = new scala.util.Random

    def getNewIdForId(oldid: Int, range : Int): Int = {
      if (mapa.contains(oldid)) {
        mapa.getOrElse(oldid, oldid)
      } else {
        var newid = rand.nextInt(range * 2)
        while (mapa.contains(newid) || newid <= 0) {
          newid = rand.nextInt(range * 2)
        }
        mapa += (oldid -> newid)
        newid
      }
    }

    var lines = Source.fromFile(args(0)).getLines
    val out = new PrintWriter(new FileWriter(new File(args(1))));

    val range = Source.fromFile(args(0)).getLines.size
    for(line <- lines if !line.isEmpty()){
      var v = line.split("\\W+")
      if (v.length != 2) {
        println("azaza")
        return
      }
      out.println(v.map(x => getNewIdForId(x.toInt, range)).mkString("\t"))  
    }

    out close

  }
}