package com.atguigu.spark.steaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/22 14:09
  */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 两个工具类: 1. KafkaUtils 2. KafkaCluster
        val params = Map[String, String](
            //            "group.id" -> "bigdata",
            //            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
            ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
        )
        val sourceSteam: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            Set("spark0919"))
        
        val resultStream = sourceSteam.flatMap {
            case (_, v) => v.split("\\W+").map((_, 1))
        }.reduceByKey(_ + _)
    
        resultStream.print(1000)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
