package com.atguigu.spark.steaming.day02.output

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/24 11:31
  */
object OutputDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OutputDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceSteam: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    
        val result = sourceSteam
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
//        result.saveAsTextFiles("log")
        
        
        val spark = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val tbtale = "stream1"
        val props = new Properties()
        props.put("user", "root")
        props.put("password", "aaaaaa")
        result.foreachRDD(rdd => {
            
            rdd.toDF("word", "count").write.mode("append")
                .jdbc(url, tbtale, props)
            /*rdd.foreachPartition(it => {
                //建立连接
               
                // 写入: 单条,批次写入
                
                
                
                
            })*/
        })
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
/*
1. 把所有的数据拉取到驱动, 在驱动端统一向mysql写入
    好处: mysql的压力很小, 只需要在驱动建立一个到msyql的连接就行了
    坏处: 如果数据量大的情况, 导致驱动端内存溢出
    
    rdd.collect()
    
2. 每个分区建立一次连接, 再去写入

3. 把rdd转换成df之后再写

 */