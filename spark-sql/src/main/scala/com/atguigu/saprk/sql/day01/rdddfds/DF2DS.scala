package com.atguigu.saprk.sql.day01.rdddfds

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 10:46
  */
object DF2DS {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Df2Rdd").setMaster("local[2]")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._
        
        val df = spark.read.json("c:/users.json")
        
        val ds= df.as[User1]
       
        val df1: DataFrame = ds.toDF
        df1.show(1000)
        
        spark.stop()
        
    }
}
case class User1(name: String, age: Long)
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
    
df->ds
    先有样例类
    df.as[样例类]
ds->df
    ds.toDF
 
   
 */