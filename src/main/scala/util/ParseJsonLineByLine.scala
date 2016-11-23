package util

import util.EventLogParser._
import spray.json._
import java.io.File
import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter

object ParseJsonLineByLine {

  case class KeyValue(
    launchTime: Long = 0,
    finishTime: Long = 0,
    jvmGCTime: Long = 0,
    executorDeserializeTime: Long = 0,
    resultSerializationTime: Long = 0,
    executorRunTime: Long = 0,
    fetchWaitTime: Long = 0,
    gettingResultTime: Long = 0,
    shuffleWriteTime: Long = 0,
    stageId: Long = 0)

  def sumation(a: KeyValue, b: KeyValue) = {
    KeyValue(a.launchTime + b.launchTime,
      a.finishTime + b.finishTime,
      a.jvmGCTime + b.jvmGCTime,
      a.executorDeserializeTime + b.executorDeserializeTime,
      a.resultSerializationTime + b.resultSerializationTime,
      a.executorRunTime + b.executorRunTime,
      a.fetchWaitTime + b.fetchWaitTime,
      a.gettingResultTime + b.gettingResultTime,
      a.shuffleWriteTime + b.shuffleWriteTime, a.stageId)

  }

  def computationTime(data: KeyValue) = {
    data.executorRunTime - data.fetchWaitTime + data.jvmGCTime
  }

  def communicationTime(data: KeyValue) = {
    data.executorDeserializeTime + data.resultSerializationTime + data.fetchWaitTime // +(data.shuffleWriteTime / 1000000)//+ data.gettingResultTime 
  }

  def allTime(data: KeyValue) = {
    data.finishTime - data.launchTime
  }

  def longFromMap(key: String, mp: Map[String, spray.json.JsValue]) = {
    val x = mp.get(key)
    if (x.isDefined) {
      x.get.asInstanceOf[JsNumber].value.toLong
    } else { 0 }
  }
  def getInfoFromFile(file: File) = {
    val list = for (line <- Source.fromFile(file).getLines().toList)
      yield jsflatter(line.parseJson)

    val ss = list.filter(x => {
      val value = x.getOrElse(".Task Info.Executor ID", JsString("0000"))
      value.asInstanceOf[JsString].value == "1"
    })

    val coll = for (mp <- ss.filter(x => !x.keys.filter(v => v.contains(".Task Metrics")).isEmpty)) yield KeyValue(
      longFromMap(".Task Info.Launch Time", mp),
      longFromMap(".Task Info.Finish Time", mp),
      longFromMap(".Task Metrics.JVM GC Time", mp),
      longFromMap(".Task Metrics.Executor Deserialize Time", mp),
      longFromMap(".Task Metrics.Result Serialization Time", mp),
      longFromMap(".Task Metrics.Executor Run Time", mp),
      longFromMap(".Task Metrics.Shuffle Read Metrics.Fetch Wait Time", mp),
      longFromMap(".Task Info.Getting Result Time", mp),
      longFromMap(".Task Metrics.Shuffle Write Metrics.Shuffle Write Time", mp),
      longFromMap(".Stage ID", mp))

    val times = coll.map(item => (communicationTime(item), computationTime(item), allTime(item), item.stageId))
    val longestInEachStage = times.groupBy(quartet => quartet._4).map(x => x._2.maxBy(i => i._3))

    List(file.getName(), longestInEachStage.map(_._1).sum, longestInEachStage.map(_._2).sum, longestInEachStage.map(_._3).sum)
  }

  def getMetricsFromFile(file: File) = {
    val list = for (line <- Source.fromFile(file).getLines().toList)
      yield jsflatter(line.parseJson)

    val ss = list.filter(x => {
      val value = x.getOrElse(".Task Info.Executor ID", JsString("0000"))
      value.asInstanceOf[JsString].value == "1"
    })

    val coll = for (mp <- ss.filter(x => !x.keys.filter(v => v.contains(".Task Metrics")).isEmpty)) yield KeyValue(
      longFromMap(".Task Info.Launch Time", mp),
      longFromMap(".Task Info.Finish Time", mp),
      longFromMap(".Task Metrics.JVM GC Time", mp),
      longFromMap(".Task Metrics.Executor Deserialize Time", mp),
      longFromMap(".Task Metrics.Result Serialization Time", mp),
      longFromMap(".Task Metrics.Executor Run Time", mp),
      longFromMap(".Task Metrics.Shuffle Read Metrics.Fetch Wait Time", mp),
      longFromMap(".Task Info.Getting Result Time", mp),
      longFromMap(".Task Metrics.Shuffle Write Metrics.Shuffle Write Time", mp),
      longFromMap(".Stage ID", mp))

    val times = coll.map(item => (communicationTime(item), computationTime(item), allTime(item), item.stageId, item))
    val longestInEachStage = times.groupBy(quartet => quartet._4).map(x => x._2.maxBy(i => i._3)).map(_._5)

    if (!longestInEachStage.isEmpty) {
      Some(longestInEachStage.reduce(sumation))
    } else {
      None
    }
  }

  def main(args: Array[String]) {
    val out = new PrintWriter(new FileWriter(args(1), true));

    val rootFolder = new File(args(0))

    for (file <- rootFolder.listFiles) {
      println("process " + file.getAbsolutePath())
      val os = getMetricsFromFile(file)
      var ar = Array(file.getName(), 0, 0, 0, 0, 0, 0)
      if (os.isDefined) {
        val s = os.get
        ar = Array(file.getName(), s.jvmGCTime, s.executorDeserializeTime, s.resultSerializationTime, s.executorRunTime, s.fetchWaitTime, s.shuffleWriteTime)
      }
      out.println(ar.mkString(";"))
      out.flush
    }

    out.close
  }
  //  def main(args: Array[String]) {
  //    val out = new PrintWriter(new FileWriter(args(1), true));
  //
  //    val rootFolder = new File(args(0))
  //
  //    for (file <- rootFolder.listFiles) {
  //      println("process " + file.getAbsolutePath())
  //      val s = getInfoFromFile(file).mkString(";")
  //      out.println(s)
  //      println(s)
  //    }
  //
  //    out.close
  //  }
}