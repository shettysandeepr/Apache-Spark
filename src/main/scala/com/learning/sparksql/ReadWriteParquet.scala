package com.learning.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ReadWriteParquet {

  val path = "D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/src/main/resources/Employee.json"

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("sqlcontext")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val employeeDF = sqlContext.read.json(path)
    employeeDF.printSchema()
    employeeDF.registerTempTable("employee")
    val employeesList = sqlContext.sql("SELECT * FROM employee")
    println("Employee List" + employeesList)

    employeeDF.write.parquet("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/output/employee.parquet")

    val empParquetDF = sqlContext.read.parquet("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/output/employee.parquet")
    empParquetDF.registerTempTable("employeeParquetDF")
    val teenagers = sqlContext.sql("SELECT * FROM employeeParquetDF WHERE age >= 32 AND age <= 33")
    teenagers.collect().foreach(println)
  }
}