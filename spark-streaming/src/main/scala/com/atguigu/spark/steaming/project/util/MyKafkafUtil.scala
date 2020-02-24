package com.atguigu.spark.steaming.project.util

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkafUtil {
    
    val map: Map[String, String] = Map[String, String]()
    def getStream(ssc: StreamingContext, topic: String) = {
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            map,
            Set(topic))
    }
}
