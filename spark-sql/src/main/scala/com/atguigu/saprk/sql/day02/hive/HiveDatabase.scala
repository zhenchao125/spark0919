package com.atguigu.saprk.sql.day02.hive

import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/2/21 10:54
  */
object HiveDatabase {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("HiveDatabase")
            .enableHiveSupport()
            // 指定创建的数据仓库的地址
            .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
            .getOrCreate()
        import spark.implicits._
        
        spark.sql("create database spark0920").show
        
        spark.close()
        
        
    }
}
/*
1. 以后在生产环境下, 创建数据库的时候, 最好在hive下提前把数据库创建好
2. 如果要在spark的代码中创建, 需要添加一个参数:
 */