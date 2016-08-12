package com.learning.sparkcore

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ParalleliseRDD {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("paralleliseRDD").setMaster("local")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    println(distData.count());
    sc.stop()
  }
}