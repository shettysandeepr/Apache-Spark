package com.learning.sparkcore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TextFileRDD {
  val path = "D:/Spark/spark-1.6.2-bin-hadoop2.6/README.md";
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("textFileRDD")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(path, 5)
    val sparkRDD = textFile.filter { x => x.contains("Spark") }
    println("Line's Which contain Spark in them")
    sparkRDD.collect().foreach(println)
    println("Total Count of Spark Words :" + sparkRDD.count())
  }
}