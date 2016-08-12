package com.learning.sparkcore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CreateSparkContext {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sparkcontext").setMaster("local")
    val sc = new SparkContext(conf)
    println(sc)
    sc.stop()
  }
}
