package com.atguigu.spark.steaming.project.app

import com.atguigu.spark.steaming.project.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

/**
  * Author atguigu
  * Date 2020/2/24 14:36
  */
object AdsClickTop3App extends App {
    override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
        // 1. 每天每地区没广告的点击量
        val dayAreaAdsAndOne: DStream[((String, String, String), Int)] =
            adsInfoStream.map(info => ((info.dayString, info.area, info.adsId), 1))
        
        // (02-24,华北,1)->1000  (02-24,华北,2)->1500
        val dayAreaAdsAndCount: DStream[((String, String, String), Int)] = dayAreaAdsAndOne.updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
            Some(seq.sum + opt.getOrElse(0))
        })
        // 2. 对每天每地区的广告的点击量进行降序, 然后再取前3
        // 2.1 按照天和地区分组
        val dayAreaGouped: DStream[((String, String), Iterable[(String, Int)])] = dayAreaAdsAndCount.map {
            case ((day, area, adsId), count) => ((day, area), (adsId, count))
        }.groupByKey()
        // 2.2 降序取前3
        val resultStream = dayAreaGouped.map {
            case ((day, area), adsIdCountIt) => {
                ((day, area), adsIdCountIt.toList.sortBy(-_._2).take(3))
            }
        }
        
        // 3. 写入到redis
        resultStream.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                // 1. 建立到redis连接
                
                // 2. 把数据写入到redis
                
                // 3. 关闭到redis的连接
            })
        })
        
    }
}

/*
// 每天每地区热门广告 Top3
1. 计算每天每地区每个广告的点击量
    带状态的统计
    // 02-24  02-25
    Stream[((dayString, area, adsId), 1)]
    

2. 每天每地区取top3

3. 把输入写入到redis


 */