package com.atguigu.spark.steaming.project.app

import com.atguigu.spark.steaming.project.bean.AdsInfo
import com.atguigu.spark.steaming.project.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author atguigu
  * Date 2020/2/24 14:37
  */
object LastHourAdsClickApp extends App {
    override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
        // 1. 先直接使用window 划分窗口
        val adsInfoStreamWithWindow: DStream[AdsInfo] = adsInfoStream.window(Minutes(60), Seconds(6))
        //2. 调整数据类型: ((adsId, hm), 1)
        val adsHmAndOne = adsInfoStreamWithWindow.map(info => ((info.adsId, info.hmString), 1))
        // 3. 3. 聚合  ((ads, hm), 100)
        val adsHmAndCount: DStream[((String, String), Int)] = adsHmAndOne.reduceByKey(_ + _)
        
        // 4. 把每个广告每分钟的点击放在一起   (ads, It(hm, count))
        val adsIdAndHmCountIt: DStream[(String, Iterable[(String, Int)])] = adsHmAndCount.map {
            case ((adsId, hm), count) => (adsId, (hm, count))
        }.groupByKey
        
        // 5. 写入到redis
        val key = "last:hour:ads"
        import org.json4s.JsonDSL._
        import scala.collection.JavaConversions._
        adsIdAndHmCountIt.foreachRDD(rdd => {
            rdd.foreachPartition((it: Iterator[(String, Iterable[(String, Int)])]) => {
                // 1.先建立连接
                val client: Jedis = RedisUtil.getClient
                
                // 2. 写入数据  (1. 单条写  2. 批次写)
                if (it.hasNext) { // it.next
                    val map = it.map{
                        case (adsId, hmCountIt) =>
                            (adsId, JsonMethods.compact(JsonMethods.render(hmCountIt)))
                    }.toMap
                    client.hmset(key, map)
                }
                
                // 3. 关闭连接
                client.close()
            })
        })
        
        
    }
}

/*
迭代器 Iterator  (Iterable)
他们是一种惰性数据结构, 并且他们的数据只能使用一次



统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量, 每6s更新一次
1.  最近一小时  窗口的长度是1小时
2. 每6s更新一次  窗口的滑动步长是6s
3. 各个广告每分钟的点击量:
        ((adsId, hm), 1)
        
4. 最后: 写入到redis

key                    value
"adsId:"+1            {"09:34":100, "09:35": 200, ....}   (json格式的字符串)

-----------------------

"last:hour:ads"       hash
                      field             value
                      
                      1                 {"09:34":100, "09:35": 200, ....}
                      2                 {"09:34":100, "09:35": 200, ....}


----

1. 先直接使用window 划分窗口
2. 调整数据类型: ((adsId, hm), 1)
3. 聚合
4. 写到redis

 */