package com.atguigu.saprk.sql.day01.datasource

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 16:24
  */
object Write {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
//            .config("spark.sql.sources.default", "json")  // 给sparksession设置参数
            .master("local[*]")
            .appName("Write")
            .getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("c:/users.json")
        
        // 写出去
//        df.write.format("json").mode(SaveMode.Append).save("users")
//        df.write.format("csv").mode(SaveMode.Overwrite).save("users")
        
        df.write.mode("append").json("users")
        
        spark.close()
        
        
    }
}
/*
通用的写:
    df.write.format().save()
    
    mode : 用来定义路径已经存在时候的处理方式

专用的写:


 */
