package com.pku.spark.topNRepeatKey

import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val N = 2
    val conf = new SparkConf().setMaster("local").setAppName("topNRepeatKey")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://localhost:9000/data/chapter2/file*")

    val bc = sc.broadcast(N)


    val uniqueData = data
      .map(str => (str.split(",")(0), str.split(",")(1).toInt))
      .reduceByKey((v1, v2) => v1 + v2).map((item => (item._2, item._1))).repartition(3)


    // 以下提供两种方法

    uniqueData.takeOrdered(bc.value)(Ordering.Tuple2(Ordering.Int.reverse,Ordering.String.reverse)).foreach(item =>
      println(item._2 +"--->"+ item._1)
    )

//    val candidate: Array[(Int, String)] = uniqueData.mapPartitions(iter => {
//      val iterator = iter.toIterator
//      var localTopN = new scala.collection.immutable.TreeMap[Int, String]()
//      while (iterator.hasNext) {
//        localTopN += iterator.next()
//        if (localTopN.size > bc.value)
//          {
//            val key = localTopN.firstKey
//            localTopN -= key
//          }
//      }
//      localTopN.toIterator
//    }).collect()
//
//    val sortedData = candidate.sortBy(kv => kv._1)(Ordering.Int.reverse)
//    var index = 0
//    for (elem <- sortedData if index < bc.value) {
//      println(elem._2 + "--->" + elem._1)
//      index += 1
//    }

    sc.stop()
  }
}
