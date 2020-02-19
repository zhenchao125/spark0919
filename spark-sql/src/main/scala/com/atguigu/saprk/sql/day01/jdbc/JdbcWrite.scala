package com.atguigu.saprk.sql.day01.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 16:58
  */
object JdbcWrite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JdbcWrite")
            .getOrCreate()
        import spark.implicits._
        
        val df: DataFrame = List(("lisi", 20), ("zs", 10), ("ww", 15)).toDF("name", "age")
        
        // 通用的写
        /*df.write.format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/rdd")
            .option("user", "root")
            .option("password", "aaaaaa")
            .option("dbtable", "user0919")
            .mode("append")
            .save()*/
    
    
        // 专用的写
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val tbtale = "user0920"
        val props = new Properties()
        props.put("user", "root")
        props.put("password", "aaaaaa")
        df.write.jdbc(url, tbtale, props)
        
        spark.close()
        
        
    }
}
