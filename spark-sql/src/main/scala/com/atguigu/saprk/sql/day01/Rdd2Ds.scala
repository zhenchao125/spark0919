package com.atguigu.saprk.sql.day01

import org.apache
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 10:46
  */
object Rdd2Ds {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Df2Rdd").setMaster("local[2]")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._
        val rdd = spark.sparkContext.parallelize(List(User("lisi", 10), User("zs", 20)))
        val ds: Dataset[User] = rdd.toDS()
        
        val rdd1: RDD[User] = ds.rdd
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
    
rdd->ds
    有样例类
    rdd.toDS
 
ds->rdd
    ds.rdd
    


 */