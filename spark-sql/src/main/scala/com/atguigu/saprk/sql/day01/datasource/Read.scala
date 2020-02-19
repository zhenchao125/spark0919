package com.atguigu.saprk.sql.day01.datasource

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 16:10
  */
object Read {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Read")
            .getOrCreate()
        import spark.implicits._
        
//        val df: DataFrame = spark.read.format("json").load("c:/users.json")
//        val df: DataFrame = spark.read.format("csv").load("c:/users.csv")
        
        val df: DataFrame = spark.read.csv("c:/users.csv")
        
        df.show(1000)
        
        
        
        spark.close()
        
        
    }
}
/*
读取数据的方式统一

read
   
   通用的读:
        spark.read.format("json").load("...")
        
  专用的读:
        spark.read.json("")
        spark.read.csv("")



 */