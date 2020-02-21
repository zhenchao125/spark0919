package com.atguigu.saprk.sql.day02.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/21 10:23
  */
object SparkHiveWite {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SparkHiveWite")
            .enableHiveSupport()
            .config(" spark.sql.hive.convertMetastoreParquet", false)
            .getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("file:///c:/users.json")
//        df.createOrReplaceTempView("user")
//        spark.sql("create table user1(age int, name string)")
        // 1. 写入方法1
//        spark.sql("insert into user1 select * from user")
        
        // 2. 直接把df的数据写入到hive中.  表可以不存在, 会自动创建
        // 顺序没有要求, 要保证当是append时, 列名要一样
        df.write.mode("append").saveAsTable("user2")
        
        // 3. 把df数据插入到hive的表中, 这个表必须提前存在
        // 插入的df的列的顺序要一致 (int  string), (int string), 不关心列名
        df.write.insertInto("user3")
        
        spark.close()
        
        
    }
}
/*
1. 使用hive的写入语句
    spark.sql("insert into user select * from user")
2. 使用...
    df.write.saveAsTable("user2")
3. 使用...
    df.write.insertInto("user3")


 */