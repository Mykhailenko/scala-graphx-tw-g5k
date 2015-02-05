package util

object s {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet

  val x = 2 + 8                                   //> x  : Int = 10

  import scala.io.Source

  import spray.json._
  import DefaultJsonProtocol._

  2 + 2                                           //> res0: Int(4) = 4

  import util.CSVChartData
  val c = new CSVChartData("astroph", "vertexCut")//> c  : util.CSVChartData = util.CSVChartData@6b38c54e
  c.addPoints("RandomHuyandom", Array[(Int, String)]((1, "2.0"), (2, "10.0")))
  c.addPoints("RandomHuyand2om", Array[(Int, String)]((1, "2.2"), (2, "104.0")))
  c.numberOfPartitioners                          //> res1: Int = 2
  c.numberOfExperiments                           //> res2: Int = 2

  c.csv                                           //> res3: String = "astroph vertexCut ,  , 
                                                  //| Number of partitions , RandomHuyandom , RandomHuyand2om
                                                  //| 1 , 2.0 , 2.2
                                                  //| 2 , 10.0 , 104.0
                                                  //| "

  var a = Array(1, 2, 3, 3, 3, 4, 5, 5, 5, 5)     //> a  : Array[Int] = Array(1, 2, 3, 3, 3, 4, 5, 5, 5, 5)
  var b = a.groupBy(identity)                     //> b  : scala.collection.immutable.Map[Int,Array[Int]] = Map(5 -> Array(5, 5, 5
                                                  //| , 5), 1 -> Array(1), 2 -> Array(2), 3 -> Array(3, 3, 3), 4 -> Array(4))

  var d = b.map(a => (a._1, a._2.length))         //> d  : scala.collection.immutable.Map[Int,Int] = Map(5 -> 4, 1 -> 1, 2 -> 1, 3
                                                  //|  -> 3, 4 -> 1)
  var t = d.toArray                               //> t  : Array[(Int, Int)] = Array((5,4), (1,1), (2,1), (3,3), (4,1))
  t.sortWith(_._1 < _._1)                         //> res4: Array[(Int, Int)] = Array((1,1), (2,1), (3,3), (4,1), (5,4))
  var h = Array((1, 100), (2, 105), (3, 100))     //> h  : Array[(Int, Int)] = Array((1,100), (2,105), (3,100))
  var g = h.groupBy(_._2)                         //> g  : scala.collection.immutable.Map[Int,Array[(Int, Int)]] = Map(100 -> Arra
                                                  //| y((1,100), (3,100)), 105 -> Array((2,105)))
  var tt = g.map(a => (a._1, a._2.length)).toArray//> tt  : Array[(Int, Int)] = Array((100,2), (105,1))
  tt.sortWith(_._1 < _._1)                        //> res5: Array[(Int, Int)] = Array((100,2), (105,1))

  import scala.io.Source
  var lines = Source.fromFile("/user/hmykhail/home/phd/dataset/astroph.txt").getLines
                                                  //> lines  : Iterator[String] = non-empty iterator
  var lis = List[Int]()                           //> lis  : List[Int] = List()
  for (i <- 0 until 500) {
    var line = lines.next
    
    var v = line.split("\t").map(Integer.parseInt)
    lis = lis :+ v(0)
  }
  var sss = lis.toSet[Int].toList                 //> sss  : List[Int] = List(84424, 63225, 106274, 127393, 89308, 60471, 94138)
  
  var mmm = Map[Int, Int] ()                      //> mmm  : scala.collection.immutable.Map[Int,Int] = Map()
   def remove(num: Int, list: List[Int]) = (list take num) ++ (list drop (num + 1))
                                                  //> remove: (num: Int, list: List[Int])List[Int]
  var rand = new scala.util.Random                //> rand  : scala.util.Random = scala.util.Random@193d22b1
  for(x <- lis){
  
    if (!mmm.contains(x)){
    	var index = rand.nextInt(sss.size)
       mmm = mmm + ( x -> sss(index))
       sss = remove(index, sss)
       
    }
    
  }
  mmm                                             //> res6: scala.collection.immutable.Map[Int,Int] = Map(84424 -> 63225, 63225 -
                                                  //| > 127393, 106274 -> 84424, 127393 -> 60471, 89308 -> 89308, 60471 -> 94138,
                                                  //|  94138 -> 106274)
  
  var a1 = Array(1,2,3,4)                         //> a1  : Array[Int] = Array(1, 2, 3, 4)
  var a2 = Array(4,5,6,54)                        //> a2  : Array[Int] = Array(4, 5, 6, 54)
  (a1, a2).zipped map ((_,_))                     //> res7: Array[(Int, Int)] = Array((1,4), (2,5), (3,6), (4,54))
  
  
  
  var s1 = Set(1, 2, 5, 6, 8)                     //> s1  : scala.collection.immutable.Set[Int] = Set(5, 1, 6, 2, 8)
  var s2 = Set(1, 6)                              //> s2  : scala.collection.immutable.Set[Int] = Set(1, 6)
  var s3 = Set(1, 2, 5, 6, 7, 8)                  //> s3  : scala.collection.immutable.Set[Int] = Set(5, 1, 6, 2, 7, 8)
  var ss = Set[Set[Int]](s1, s2, s3)              //> ss  : scala.collection.immutable.Set[Set[Int]] = Set(Set(5, 1, 6, 2, 8), Se
                                                  //| t(1, 6), Set(5, 1, 6, 2, 7, 8))
  var qq = ss.foldLeft(List[Int]())((a, b) => a.toList ++ b.toList)
                                                  //> qq  : List[Int] = List(5, 1, 6, 2, 8, 1, 6, 5, 1, 6, 2, 7, 8)
  var tr = qq.groupBy(identity).mapValues(v => v.length)
                                                  //> tr  : scala.collection.immutable.Map[Int,Int] = Map(5 -> 2, 1 -> 3, 6 -> 3,
                                                  //|  2 -> 2, 7 -> 1, 8 -> 2)
  tr.filter(a => a._2 == 1).map(x => x._1)        //> res8: scala.collection.immutable.Iterable[Int] = List(7)
  
  
  Array[Double](5,1,4).toList.map(e => Math.pow(e * 3 / 10 - 1, 2))
                                                  //> res9: List[Double] = List(0.25, 0.48999999999999994, 0.03999999999999998)
  Math.sqrt(Array[Double](5,1,4).toList.map(e => Math.pow(e * 3 / 10 - 1, 2)).sum / 3)
                                                  //> res10: Double = 0.5099019513592785
  var aaa = Array("data.txt", "data_1.txt", "data_4.txt", "dsa.txt", "dsa_1.txt", "tttt.txt", "wqe.txt")
                                                  //> aaa  : Array[String] = Array(data.txt, data_1.txt, data_4.txt, dsa.txt, dsa
                                                  //| _1.txt, tttt.txt, wqe.txt)
  
  aaa.groupBy(x => x.substring(0, 3))             //> res11: scala.collection.immutable.Map[String,Array[String]] = Map(ttt -> Ar
                                                  //| ray(tttt.txt), wqe -> Array(wqe.txt), dsa -> Array(dsa.txt, dsa_1.txt), dat
                                                  //|  -> Array(data.txt, data_1.txt, data_4.txt))
  "da-ta-22.txt".matches(".*-[0-9]+.txt")         //> res12: Boolean = true
  import java.util.regex.Pattern._
  import java.util.regex.Pattern
	var p = Pattern.compile("([^_0123456789]*)[_0123456789]*")
                                                  //> p  : java.util.regex.Pattern = ([^_0123456789]*)[_0123456789]*
  var m = p.matcher("email-enronD_3")             //> m  : java.util.regex.Matcher = java.util.regex.Matcher[pattern=([^_01234567
                                                  //| 89]*)[_0123456789]* region=0,14 lastmatch=]
  m.find()                                        //> res13: Boolean = true
  m.group(1)                                      //> res14: String = email-enronD
  
  RaportMaker.pezdoit("/user/hmykhail/home/phd/reports/report040215/report040215")
                                                  //> at least we have data [Lscala.Tuple2;@1f78e951
                                                  //| at least we have data [Lscala.Tuple2;@4fa8b45d
                                                  //| at least we have data [Lscala.Tuple2;@83805f9
                                                  //| at least we have data [Lscala.Tuple2;@76d523bb
                                                  //| at least we have data [Lscala.Tuple2;@30634e7
                                                  //| at least we have data [Lscala.Tuple2;@49812ab7
                                                  //| at least we have data [Lscala.Tuple2;@4750cf24
                                                  //| at least we have data [Lscala.Tuple2;@637a9eb9
                                                  //| at least we have data [Lscala.Tuple2;@6623a0d3
                                                  //| at least we have data [Lscala.Tuple2;@74c314a4
                                                  //| at least we have data [Lscala.Tuple2;@427a9f36
                                                  //| at least we have data [Lscala.Tuple2;@7ebe9943
                                                  //| going to right string with 318232
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}