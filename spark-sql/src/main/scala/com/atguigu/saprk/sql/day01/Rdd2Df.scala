package com.atguigu.saprk.sql.day01

import org.apache.spark
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 10:29
  */
object Rdd2Df {
    def main(args: Array[String]): Unit = {
        // 1. 创建sparkSession
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("Rdd2Df")
            .getOrCreate()
        import spark.implicits._
        // 2. 创建rdd
       /* val list1 = List(("lisi", 20), ("zs", 30))
        val rdd= spark.sparkContext.parallelize(list1)
        val df: DataFrame = rdd.toDF("name", "age")
        */
        val users: List[User] = User("lisi", 20)::User("zs", 10)::Nil
        val rdd: RDD[User] = spark.sparkContext.parallelize(users)
        val df: DataFrame = rdd.toDF
        
        df.show(100)
        
        spark.stop()
        
    }
}

case class User(name: String, age: Int)
/*
RDD->DF
    rdd.toDF
    
    RDD中的存储的是 样例类, 转起来特别方便


df->rdd


 */