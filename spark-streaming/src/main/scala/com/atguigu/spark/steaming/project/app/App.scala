package com.atguigu.spark.steaming.project.app

import com.atguigu.spark.steaming.project.util.MyKafkafUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/24 14:38
  */
trait App {
    def main(args: Array[String]): Unit = {
        // 读kafka
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("App")
        val ssc = new StreamingContext(conf, Seconds(3))
        val sourceStream = MyKafkafUtil.getStream(ssc, "ads_log0919")
        
        
    
        // 数据封装
        
        // ?
        doSomething()
    
        // 开启流
        // 阻止主线程退出
        ssc.start()
        ssc.awaitTermination()
    }
    
    def doSomething(): Unit
}
