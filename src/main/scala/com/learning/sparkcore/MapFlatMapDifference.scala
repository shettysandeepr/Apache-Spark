package com.learning.sparkcore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MapFlatMapDifference {

  val path = "D:/Spark/spark-1.6.2-bin-hadoop2.6/README.md";

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mapTransformations")
    val sc = new SparkContext(conf)
    val textFileRDD = sc.textFile(path, 3)

    val wordsWithMapRDD = textFileRDD.map(line => line.split(" ")).coalesce(1)
    val wordsWithFlatMapRDD = textFileRDD.flatMap(line => line.split(" ")).coalesce(1)

    wordsWithMapRDD.saveAsTextFile("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/wordsWithMap")
    wordsWithFlatMapRDD.saveAsTextFile("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/wordsWithFlatMap")
  }
}