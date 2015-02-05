package util

object newone {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  val s = "10, 10, 11, 12, 12, 11, 11, 10, 9".split(", ")
                                                  //> s  : Array[String] = Array(10, 10, 11, 12, 12, 11, 11, 10, 9)

  s.groupBy(identity).map(x => (x._1, x._2.length.toDouble / s.length))
                                                  //> res0: scala.collection.immutable.Map[String,Double] = Map(12 -> 0.2222222222
                                                  //| 222222, 11 -> 0.3333333333333333, 9 -> 0.1111111111111111, 10 -> 0.333333333
                                                  //| 3333333)

  import spray.json._
  import DefaultJsonProtocol._
  import java.io.PrintWriter
  import java.io.FileWriter

  val json = "/user/hmykhail/home/phd/reports/report040215/astrophD_10/RandomVertexCut/100.json"
                                                  //> json  : String = /user/hmykhail/home/phd/reports/report040215/astrophD_10/Ra
                                                  //| ndomVertexCut/100.json
  val mapa = scala.io.Source.fromFile(json).mkString.parseJson.convertTo[Map[String, String]]
                                                  //> mapa  : Map[String,String] = Map(ResultSaving.result -> bien, name -> diamet
                                                  //| er.PartitionerTester, communicationCost -> 303627, AlgorithExecution.result 
                                                  //| -> bien, master -> spark://orion-1.lyon.grid5000.fr:7077, edgeInPartitiones 
                                                  //| -> 7944, 8188, 8230, 8420, 8124, 7888, 7780, 7776, 8140, 8008, 7792, 7696, 7
                                                  //| 992, 7636, 7852, 7672, 7896, 7528, 7856, 7902, 7872, 8098, 7970, 8004, 7882,
                                                  //|  7968, 7820, 8192, 7996, 8016, 8228, 7922, 7708, 8088, 8124, 7480, 7742, 785
                                                  //| 6, 7802, 7990, 7806, 8156, 7674, 7760, 8364, 7792, 7730, 7964, 8000, 7748, 7
                                                  //| 952, 7888, 8136, 7976, 8098, 7888, 7996, 8168, 8014, 7978, 7998, 7820, 7790,
                                                  //|  8094, 7892, 8044, 8056, 7908, 7914, 8284, 7970, 7846, 8082, 8060, 7686, 809
                                                  //| 8, 8026, 7732, 7852, 7802, 7672, 7736, 7944, 7644, 7708, 7618, 8024, 8352, 7
                                                  //| 634, 7712, 7848, 7856, 7812, 7764, 7948, 8320, 7876, 7890, 7872, 7970, Parti
                                                  //| tioning.result -> bien, AlgorithExecution.end -> 1423045083580, Partitioning
                                                  //| .start -> 1423045070168,
                                                  //| Output exceeds cutoff limit.
  val parts = mapa.getOrElse("edgeInPartitiones", "").split(", ").map(x => x.toInt).toList
                                                  //> parts  : List[Int] = List(7944, 8188, 8230, 8420, 8124, 7888, 7780, 7776, 81
                                                  //| 40, 8008, 7792, 7696, 7992, 7636, 7852, 7672, 7896, 7528, 7856, 7902, 7872, 
                                                  //| 8098, 7970, 8004, 7882, 7968, 7820, 8192, 7996, 8016, 8228, 7922, 7708, 8088
                                                  //| , 8124, 7480, 7742, 7856, 7802, 7990, 7806, 8156, 7674, 7760, 8364, 7792, 77
                                                  //| 30, 7964, 8000, 7748, 7952, 7888, 8136, 7976, 8098, 7888, 7996, 8168, 8014, 
                                                  //| 7978, 7998, 7820, 7790, 8094, 7892, 8044, 8056, 7908, 7914, 8284, 7970, 7846
                                                  //| , 8082, 8060, 7686, 8098, 8026, 7732, 7852, 7802, 7672, 7736, 7944, 7644, 77
                                                  //| 08, 7618, 8024, 8352, 7634, 7712, 7848, 7856, 7812, 7764, 7948, 8320, 7876, 
                                                  //| 7890, 7872, 7970)
  val data = parts.groupBy(identity).map(x => (x._1, x._2.length.toDouble / parts.length.toDouble)).toArray.sortWith(_._1 < _._1)
                                                  //> data  : Array[(Int, Double)] = Array((7480,0.01), (7528,0.01), (7618,0.01), 
                                                  //| (7634,0.01), (7636,0.01), (7644,0.01), (7672,0.02), (7674,0.01), (7686,0.01)
                                                  //| , (7696,0.01), (7708,0.02), (7712,0.01), (7730,0.01), (7732,0.01), (7736,0.0
                                                  //| 1), (7742,0.01), (7748,0.01), (7760,0.01), (7764,0.01), (7776,0.01), (7780,0
                                                  //| .01), (7790,0.01), (7792,0.02), (7802,0.02), (7806,0.01), (7812,0.01), (7820
                                                  //| ,0.02), (7846,0.01), (7848,0.01), (7852,0.02), (7856,0.03), (7872,0.02), (78
                                                  //| 76,0.01), (7882,0.01), (7888,0.03), (7890,0.01), (7892,0.01), (7896,0.01), (
                                                  //| 7902,0.01), (7908,0.01), (7914,0.01), (7922,0.01), (7944,0.02), (7948,0.01),
                                                  //|  (7952,0.01), (7964,0.01), (7968,0.01), (7970,0.03), (7976,0.01), (7978,0.01
                                                  //| ), (7990,0.01), (7992,0.01), (7996,0.02), (7998,0.01), (8000,0.01), (8004,0.
                                                  //| 01), (8008,0.01), (8014,0.01), (8016,0.01), (8024,0.01), (8026,0.01), (8044,
                                                  //| 0.01), (8056,0.01), (8060,0.01), (8082,0.01), (8088,0.01), (8094,0.01), (809
                                                  //| 8,0.03), (8124,0.02), (8
                                                  //| Output exceeds cutoff limit.
  var res = ""                                    //> res  : String = ""
  for((n, prob) <- data){
    res += n.toString + " , " + prob + "\n"
  }
  val out = new PrintWriter(new FileWriter(json + "stat"));
                                                  //> out  : java.io.PrintWriter = java.io.PrintWriter@1283bbd2

  out.print(res)
  out.close


}