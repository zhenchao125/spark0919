package com.atguigu.spark.steaming.day02.unstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/24 9:29
  */
object TransformDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        
        // 是个转换算子
        val result = sourceStream.transform(rdd =>{
            // 一些操作stream没有, 或者你更喜欢用rdd操作时候, 使用这个可以立即转成rdd相关操作
            rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
            
        })
        result.print(1000)
        // 行动算子 流中的数据向写入到外部存储的时候: jdbc, hive, hbase..., 使用这个
        sourceStream.foreachRDD(rdd => {
            // 在驱动的位置
            
            rdd.foreachPartition(it => {
                // 建立到外部的连接
                
                it.foreach(ele => {
                    // 真正的向外写数据
                })
                // 批处理写
            })
        })
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
/*

 */