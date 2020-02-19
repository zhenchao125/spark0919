package com.atguigu.saprk.sql.day01.rdddfds

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 11:19
  */
object DSDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DSDemo")
            .getOrCreate()
        import spark.implicits._
        
        /*val list1 = List(30, 50, 70, 60, 10, 20)
        val ds: Dataset[Int] = list1.toDS()
        ds.show(100)*/
        
        val list = List(User("lisi", 10), User("zs", 20))
        val ds: Dataset[User] = list.toDS()
        ds.show(100)
        spark.close()
        
        
    }
}
