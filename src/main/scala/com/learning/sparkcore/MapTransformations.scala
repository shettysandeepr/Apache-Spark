package com.learning.sparkcore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MapTransformations {

  val path = "D:/Spark/spark-1.6.2-bin-hadoop2.6/README.md";

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("mapTransformations")
    val sc = new SparkContext(conf)
    val textFileRDD = sc.textFile(path, 3)

    val lineLengthsRDD = textFileRDD.map { x => x.length() }
    println("Line Length Count")
    lineLengthsRDD.collect().foreach { println }

    val appendOpRDD = textFileRDD.map { x => (x, "Initial Spark") }
    appendOpRDD.collect().foreach { println }

    val totalLength = textFileRDD.reduce { (a, b) => a + b }
    println("Total Word Length :" + totalLength)

  }
}