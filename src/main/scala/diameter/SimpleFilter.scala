package diameter

import java.io.BufferedReader
import java.io.FileReader
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.PrintWriter

object SimpleFilter {
  def main(args: Array[String]) {
    require(args.length == 3, "Usage: <graph> <flatGraph> [how many edges use in percentage from 0.0 till 1.0]")
    val br = new BufferedReader(new FileReader(args(0)))
    val bw = new PrintWriter(new BufferedWriter(new FileWriter(args(1))))
    val percentage: Double = args(2).toDouble

    var line = br.readLine()
    while (line != null) {
      if (math.random < percentage) {
        bw.println(line)
      }
      line = br.readLine()
    }
    br.close()
    bw.close()
  }
}