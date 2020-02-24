package com.atguigu.spark.steaming.project.app
import com.atguigu.spark.steaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

/**
  * Author atguigu
  * Date 2020/2/24 14:37
  */
object LastHourAdsClickApp extends App{
    override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
    
    }
}
