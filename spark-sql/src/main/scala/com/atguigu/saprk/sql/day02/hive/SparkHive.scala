package com.atguigu.saprk.sql.day02.hive

import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/2/21 10:14
  */
object SparkHive {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SparkHive")
            // 支持外置hive
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        
        spark.sql("use gmall")
        spark.sql("select count(*) from dwd_user_info").show
        
        spark.close()
    }
}
/*
1. 默认spark-session不支持hive
2. 添加spark-hive依赖
3. 如果仍然有问题, 则需要把core-site.xml和hdfs-site.xml copy过来
 */
