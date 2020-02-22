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
object WordCount2 {
    def createSSC(): StreamingContext = {
        println("createSSC")
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("ck1")
        val params = Map[String, String](
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
        ssc
    }
    
    def main(args: Array[String]): Unit = {
        
        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("ck1", createSSC)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
