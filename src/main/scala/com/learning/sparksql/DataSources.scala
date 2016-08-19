package com.learning.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DataSources {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("sqlcontext")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val usersDF = sqlContext.read.load("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/src/main/resources/users.parquet")
    usersDF.select("name", "favorite_color").write.save("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/output/namesAndFavColors.parquet")

    val peopleDF = sqlContext.read.format("json").load("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/src/main/resources/people.json")
    peopleDF.select("name", "age").write.format("parquet").save("D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/output/namesAndAges.parquet")

    val df = sqlContext.sql("SELECT * FROM parquet.`D:/Spark/spark-1.6.2-bin-hadoop2.6/examples/src/main/resources/users.parquet`")
    df.show()
  }
}