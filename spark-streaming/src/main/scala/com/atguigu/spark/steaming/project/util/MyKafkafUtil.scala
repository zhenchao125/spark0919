package com.atguigu.spark.steaming.project.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkafUtil {
    
    val params = Map[String, String](
        ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    )
    def getStream(ssc: StreamingContext, topic: String) = {
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            Set(topic))
    }
}
