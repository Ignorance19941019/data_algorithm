package com.pku.spark.leftOutJoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LeftOuterJoin1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("leftouterjoin").setMaster("local")
    val sc = new SparkContext(conf)

    val usersRDD = sc.textFile("hdfs://localhost:9000/data/chapter4/users")
    val transactionsRDD = sc.textFile("hdfs://localhost:9000/data/chapter4/transactions")

    val usersPairRDD = usersRDD.map(line => {
      val spilt = line.split(" ")
      val user = spilt(0)
      val location = spilt(1) + "-" + "L"
      (user, location)
    }
    )
    val transactionsPairRDD = transactionsRDD.map(line => {
      val spilt = line.split(" ")
      val user = spilt(2)
      val product = spilt(1) + "-" + "P"
      (user, product)
    })
    val groupByUser = transactionsPairRDD.union(usersPairRDD).groupByKey()
    val user_product_location: RDD[(String, Iterator[(String, String)])] = groupByUser.map(item => {
      val user = item._1
      val iter = item._2
      val productAndLocation = ArrayBuffer[(String, String)]()
      val array = iter.toArray
      for (i <- array.indices) {
        val split1 = array(i).split("-")
        for (j <- (i + 1).until(array.length)) {
          val split2 = array(j).split("-")
          if (!split1(1).equals(split2(1))) {
            if (split1(1).equals("P"))
              productAndLocation.append((split1(0), split2(0)))
            else
              productAndLocation.append((split2(0), split1(0)))
          }
        }
      }
      (user, productAndLocation.toIterator)
    })

    user_product_location.map(_._2).flatMap(_.toIterator).groupByKey().map(item => {
      val set = mutable.HashSet[String]()
      val product = item._1
      val iterLocation = item._2.toIterator
      while (iterLocation.hasNext) {
        set.add(iterLocation.next())
      }
      var locations = ""
      set.foreach(location =>
        locations = locations + location + ","
      )
      (product, (locations, set.size))
    }
    ).foreach(item => {
      println(item._1 + "(" + item._2._1 + item._2._2 + ")")
    })

    user_product_location.foreach(item => {
      val iter = item._2
      while (iter.hasNext) {
        val product_location = iter.next()
        println(product_location._1 + " " + product_location._2)
      }
    })
  }

}
