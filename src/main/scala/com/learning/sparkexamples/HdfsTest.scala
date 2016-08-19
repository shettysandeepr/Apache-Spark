package com.learning.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HdfsTest {

  val path = "D:/Spark/spark-1.6.2-bin-hadoop2.6/README.md";

  /** Usage: HdfsTest [file] */
  def main(args: Array[String]) {
//    if (args.length < 1) {
//      System.err.println("Usage: HdfsTest <file>")
//      System.exit(1)
//    }
    val sparkConf = new SparkConf().setAppName("HdfsTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(path)
    val mapped = file.map(s => s.length).cache()
    for (iter <- 1 to 10) {
      val start = System.currentTimeMillis()
      for (x <- mapped) { x + 2 }
      val end = System.currentTimeMillis()
      println("Iteration " + iter + " took " + (end - start) + " ms")
    }
    sc.stop()
  }
}