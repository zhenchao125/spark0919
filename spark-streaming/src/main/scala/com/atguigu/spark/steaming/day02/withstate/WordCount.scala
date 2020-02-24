package com.atguigu.spark.steaming.day02.withstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/24 9:55
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("ck2")
        val sourceSteam: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        val result = sourceSteam
        .flatMap(_.split(" "))
            .map((_, 1))
            // 这个函数有致命的问题,导致在公司用的很少
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
                Some(seq.sum + opt.getOrElse(0))
            })
        result.print(10000)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
/*

1. updateStateByKey
    记住从启动开始的状态
    
    从系统启动所有数据都可以在一起做聚合
    
2. window 窗口


 */