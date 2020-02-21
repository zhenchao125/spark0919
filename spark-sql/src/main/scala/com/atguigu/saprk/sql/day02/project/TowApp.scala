package com.atguigu.saprk.sql.day02.project

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/21 16:20
  */
object TowApp {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("TowApp")
            .enableHiveSupport()
            .config("spark.sql.shuffle.partitions", "10")
            .getOrCreate()
        import spark.implicits._
    
        spark.sql("use sql0919")
        val df: DataFrame = spark.sql("select count(*) count from city_info group by area")
        df.write.saveAsTable("c")
//        println(df.rdd.getNumPartitions)
        
        spark.close()
        
        
    }
}
