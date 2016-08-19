package com.learning.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object ReadWriteJson {
  val jsonPath = "D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/src/main/resources/people.json"

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("sqlcontext")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    println("SQLContext Created successfully :" + sqlContext)

    val jsonDF = sqlContext.read.json(jsonPath)
    jsonDF.cache()
    println("JSON Opeartions to follow:")
    jsonDF.show()
    jsonDF.printSchema()
    jsonDF.select("name").show()
    jsonDF.select(jsonDF("name"), jsonDF("age") + 1).show()
    jsonDF.select(jsonDF("age") > 21).show()
    jsonDF.groupBy("age").count().show()
    jsonDF.registerTempTable("people")
    val ageDF = sqlContext.sql("select * from people where age > 21")
    ageDF.show()
  }
}