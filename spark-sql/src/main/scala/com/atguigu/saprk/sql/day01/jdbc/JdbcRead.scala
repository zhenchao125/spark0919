package com.atguigu.saprk.sql.day01.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/2/19 16:46
  */
object JdbcRead {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JdbcRead")
            .getOrCreate()
        
        /*val df = spark.read.format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/rdd")
            .option("user", "root")
            .option("password", "aaaaaa")
            .option("dbtable", "user2")
            .load()*/
        
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val tbtale = "user2"
        val props = new Properties()
        props.put("user", "root")
        props.put("password", "aaaaaa")
        val df = spark.read.jdbc(url, tbtale, props)
        df.show(1000)
        
        spark.close()
    }
}

/*
通用的:

专用:


 */