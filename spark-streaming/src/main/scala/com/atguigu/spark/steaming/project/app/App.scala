package com.atguigu.spark.steaming.project.app

import com.atguigu.spark.steaming.project.bean.AdsInfo
import com.atguigu.spark.steaming.project.util.MyKafkafUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
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
        ssc.checkpoint("ck1")
        val sourceStream: InputDStream[(String, String)] = MyKafkafUtil.getStream(ssc, "ads_log0919")
        // 1582525949598,华北,北京,105,3
        val adsInfoStream: DStream[AdsInfo] = sourceStream.map {
            case (_, value) =>
                val s: Array[String] = value.split(",")
                AdsInfo(s(0).toLong, s(1), s(2), s(3), s(4))
        }
        
        
        // 数据封装
        
        // ?
        doSomething(adsInfoStream)
        
        // 开启流
        // 阻止主线程退出
        ssc.start()
        ssc.awaitTermination()
    }
    
    def doSomething(adsInfoStream: DStream[AdsInfo]): Unit
}
