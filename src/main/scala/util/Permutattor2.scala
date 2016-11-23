package util

import scala.io.Source
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Permutattor2 {
  def main(args: Array[String]) {
    require(args.length == 2, "Wrong argument number. Should be 2. Usage: <path_to_grpah> <path_to_permutatted>")

    var mapa = Map[Long, Long]()
    var sset = Set[Long]()
    var rand = new scala.util.Random
    val range = Source.fromFile(args(0)).getLines.size

    var numberOfVertices = 0
    def getNewIdForId(oldid: Long): Long = {
      if (mapa.contains(oldid)) {
        mapa.get(oldid).get
      } else {
        var newid = rand.nextInt
        while (sset.contains(newid) || newid <= 0) {
          newid = rand.nextInt
        }
        mapa += (oldid -> newid)
        sset += newid
        numberOfVertices = numberOfVertices + 1
        newid
      }
    }

    var lines = Source.fromFile(args(0)).getLines
    val out = new PrintWriter(new FileWriter(new File(args(1))));

    val percent = range / 100;
    def printProgress(i : Int) : Unit = {
      if(i % percent == 0){
        if (i != 0){
          println("Processed: " + (i / percent) + "%")
        }
      }
    }
    var i = 0
    for(line <- lines if !line.isEmpty()){
      printProgress(i)

      var v = line.split("\\W+")

      require(v.length == 2, "Wrong line format: " + line)

      val sourceId = getNewIdForId(v(0).toLong)
      val destinationId = getNewIdForId(v(1).toLong)

      out.println(sourceId + " " + destinationId)

      i = i + 1
    }
    println("number of vertices is " + numberOfVertices)

    out close

  }

}
