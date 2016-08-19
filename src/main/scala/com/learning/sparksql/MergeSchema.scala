package com.learning.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object MergeSchema {
  val path = "D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/src/main/resources/Employee.json"

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("sqlcontext")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.parquet("data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.parquet("data/test_table/key=2")

    // Read the partitioned table
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
    df3.printSchema()
  }
}