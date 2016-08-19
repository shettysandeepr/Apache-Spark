package com.learning.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ReflexionSchemaInference {

  def main(args: Array[String]) {
//    val conf = new SparkConf().setMaster("local").setAppName("sqlcontext")
//    val sc = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
//
//    case class Person(name: String, age: Int)
//
//    val peopleRDD = sc.textFile("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/src/main/resources/people.txt").map(_.split(",")).map { p => Person(p(0).trim(), p(1).trim().toInt) }.toDF()
//
//    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
//    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
//
//    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)
//
//    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
//    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
//    // Map("name" -> "Justin", "age" -> 19)	
  }
}