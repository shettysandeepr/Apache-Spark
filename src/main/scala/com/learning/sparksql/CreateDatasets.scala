package com.learning.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
object CreateDatasets {

  def main(args: Array[String]) {
   
//    val conf = new SparkConf().setMaster("local").setAppName("sqlcontext")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    println("SQLContext Created successfully :" + sqlContext)
//
//    // this is used to implicitly convert an RDD to a DataFrame.
//    import sqlContext.implicits._
//
//    // Encoders for most common types are automatically provided by importing sqlContext.implicits._
//    val intDS = Seq(1, 2, 3).toDS()
//    intDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
//
//     // Encoders are also created for case classes.
//    case class Person(name: String, age: Long)
////    val personDS = Seq(Person("Andy", 32)).toDS()
//
//    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
//    val path = "examples/src/main/resources/people.json"
//    val people = sqlContext.read.json(path).as[Person]
//    people.show()

  }
}