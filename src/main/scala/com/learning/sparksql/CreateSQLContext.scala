package com.learning.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object CreateSQLContext {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("sqlcontext")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    println("SQLContext Created successfully :" + sqlContext)
  }
}

