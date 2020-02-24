package com.atguigu.spark.steaming.day02.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/24 10:34
  */
object WordCount2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("ck2")
        val sourceSteam = ssc.socketTextStream("hadoop102", 9999)
            .window(Seconds(9), Seconds(6))
        
        
        val result = sourceSteam
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
//
        result.print(1000)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
updateStateByKey
    从启动到当前的整体状态


Window
比如需要每6s统计一次最近5分内的 wordcount
窗口长度:  5分钟
窗口的步长: 6s

 */
