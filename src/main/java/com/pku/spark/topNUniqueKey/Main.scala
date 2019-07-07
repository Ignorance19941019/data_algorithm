package com.pku.spark.topNUniqueKey


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.TreeMap

/*
获取按照某个value排序的结果
 */

object Main {

  var N = 10
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)
  val bc: Broadcast[Int] = sc.broadcast(N)

  def main(args: Array[String]): Unit = {

    //模拟从不同的文件读取，模拟2个RDD
    val data1 = sc.textFile("hdfs://localhost:9000/data/chapter2/top10data")
    val data2 = sc.textFile("hdfs://localhost:9000/data/chapter2/top10data2")
    val data = data1.union(data2)
    val candidateData: Array[(Float, String)] = data.mapPartitions(topNFunction).collect()
    println(candidateData.size)
    candidateData.foreach(item => println(item._2 + "--->" + item._1))
    println("-----------------------------------------------")
    var finalTopN: TreeMap[Float, String] = new TreeMap[Float, String]()

    candidateData.foreach(kv => {
      finalTopN += (kv._1 -> kv._2)
      if (finalTopN.size > bc.value) {
        val key = finalTopN.firstKey
        finalTopN -= key
      }
    }
    )

    finalTopN.foreach(x => println(x._2 + "--->" + x._1))
    sc.stop()
  }

  def topNFunction(iter: Iterator[String]): Iterator[(Float, String)] = {
    var topNCats: TreeMap[Float, String] = new TreeMap[Float, String]()
    iter.foreach(elem => {
      val kv: Array[String] = elem.split(",")
      topNCats += (kv(1).toFloat -> kv(0))
      if (topNCats.size > bc.value) {
        val key = topNCats.firstKey
        topNCats -= key
      }
    }
    )
    topNCats.toIterator
  }
}
