package com.atguigu.spark.steaming.project.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.spark.steaming.project.bean.AdsInfo
import com.atguigu.spark.steaming.project.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

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
            rdd.foreachPartition((it: Iterator[((String, String), List[(String, Int)])]) => {
                // 1. 建立到redis连接
                val client: Jedis = RedisUtil.getClient
                // 2. 把数据写入到redis
                /*
                key                                             value
                每天一个key                                      hash
                "day:area:ads:"2020-02-24                       field           value
                                                                "华北"          {"3": 50, "2": 47, "1": 47}
                 
                 */
                // 单条写入
                /*it.foreach{
                    case ((day, area), list) =>
                        val key = "day:area:ads:" + day
                        val field = area
                        // json4s  json for scala
                        import org.json4s.JsonDSL._
                        val value = JsonMethods.compact(JsonMethods.render(list))
                        client.hset(key, field, value)
                }*/
                // 批次写入
                import org.json4s.JsonDSL._
                if (it.hasNext) {
                    
                    val key = "day:area:ads:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
                    // map
                    // Iterator[((String, String), List[(String, Int)])]
                    // 有一些隐式转换,架起了scala的集合和java的集合之间的桥梁
                    import scala.collection.JavaConversions._
                    val areaAndAdsCout: Map[String, String] = it.map {
                        case ((_, area), list) => (area, JsonMethods.compact(JsonMethods.render(list)))
                    }.toMap
                    client.hmset(key, areaAndAdsCout)
                }
                
                // 3. 关闭到redis的连接
                RedisUtil.close(client)
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

3. 把输出入到redis

((2020-02-24,华北),List((3,50), (2,47), (1,47)))
((2020-02-24,华南),List((1,50), (2,46), (3,44)))
((2020-02-24,华中),List((5,14), (4,12), (3,12)))
((2020-02-24,华东),List((1,47), (2,39), (4,38)))

key                                             value
每天一个key                                      hash
"day:area:ads:"2020-02-24                       field           value
                                                "华北"          {"3": 50, "2": 47, "1": 47}
 
 key-value no-sql数据库
 value5大数据类型:
    string
    
    list
    
    set
        去重用
    
    hash
        key-value (map)
        
    带分数的list
    
 



 */