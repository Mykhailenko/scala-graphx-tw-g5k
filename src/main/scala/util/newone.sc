package util

import sun.org.mozilla.javascript.ast.Yield

object newone {
  val qwe = List(1, 1, 1, 2, 2, 3, 3, 3)          //> qwe  : List[Int] = List(1, 1, 1, 2, 2, 3, 3, 3)
  val qweq = qwe.groupBy(identity(_)).map(x => (x._1, x._2.length)).toArray
                                                  //> qweq  : Array[(Int, Int)] = Array((2,2), (1,3), (3,3))
  qwe.filter(x => math.random < 0.1)              //> res0: List[Int] = List()

  2 + 2                                           //> res1: Int(4) = 4

  def zip(a: List[List[Int]], b: List[List[Int]], default: List[Int] = List(0, 0, 0)): List[List[Int]] = {
    val maxLenght = List(a.length, b.length).max

    def get(lst: List[List[Int]], index: Int): List[Int] = {
      if (0 <= index && index < lst.length) lst(index) else default
    }

    val res = for (i <- 0 until maxLenght) yield get(a, i) ++ get(b, i)
    res.toList

  }                                               //> zip: (a: List[List[Int]], b: List[List[Int]], default: List[Int])List[List[I
                                                  //| nt]]
  val s = "-partitionandpagerank-twitter_1.txt-edgepartition1d-7-cores-1434447778082"
                                                  //> s  : String = -partitionandpagerank-twitter_1.txt-edgepartition1d-7-cores-14
                                                  //| 34447778082
  val ar = s.split("-")                           //> ar  : Array[String] = Array("", partitionandpagerank, twitter_1.txt, edgepar
                                                  //| tition1d, 7, cores, 1434447778082)
  ar(1).substring("partitionand".length)          //> res2: String = pagerank
  val ww = ar(2).split("_")                       //> ww  : Array[String] = Array(twitter, 1.txt)
  ww(0)                                           //> res3: String = twitter
  ww(1).replaceAll("[^\\d]", "").toInt            //> res4: Int = 1
  ar(3)                                           //> res5: String = edgepartition1d
	class A {
	

	}	
}