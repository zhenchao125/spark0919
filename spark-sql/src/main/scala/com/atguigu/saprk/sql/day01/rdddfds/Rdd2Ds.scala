package com.atguigu.saprk.sql.day01.rdddfds

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 10:46
  */
object Rdd2Ds {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Df2Rdd").setMaster("local[2]")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._
        val rdd = spark.sparkContext.parallelize(List(User("lisi", 10), User("zs", 20)))
        val ds: Dataset[User] = rdd.toDS()
        
        val rdd1: RDD[User] = ds.rdd
        rdd1.collect.foreach(println)
        
        spark.stop()
        
    }
}
