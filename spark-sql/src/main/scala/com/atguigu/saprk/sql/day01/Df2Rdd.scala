package com.atguigu.saprk.sql.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 10:46
  */
object Df2Rdd {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Df2Rdd").setMaster("local[2]")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        spark.sparkContext.setLogLevel("error")
        // 1. 先得到df
        val df: DataFrame = spark.read.json("c:/users.json")
        
        val rdd: RDD[Row] = df.rdd
        
        val rdd1 = rdd.map(row => {
            row.getString(1)
        })
        rdd1.collect.foreach(println)
        
        spark.stop()
    }
}
/*
RDD->DF
    rdd.toDF
    
    RDD中的存储的是 样例类, 转起来特别方便


df->rdd
    df.rdd
    得到的rdd中的数据类型一定是 Row


 */