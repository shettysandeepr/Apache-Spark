package com.learning.spark.examples
import org.apache.spark.{ SparkConf, SparkContext }

object ExceptionHandlingTest {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("ExceptionHandlingTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    sc.parallelize(0 until sc.defaultParallelism).foreach { i =>
      if (math.random > 0.75) {
        throw new Exception("Testing exception handling")
      }
    }
    sc.stop()
  }
}