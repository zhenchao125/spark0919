package com.atguigu.spark.steaming.day01.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/22 14:09
  */
object WordCount3 {
    val params = Map[String, String](
        ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    )
    
    // KafkaCluster
    val cluster = new KafkaCluster(params)
    val topics = Set("spark0919")
    val groupId = "bigdata"
    
    
    // 读取上次保存的偏移量的值, 这次就可以从这些位置开始消费
    def readOffsets(): Map[TopicAndPartition, Long] = {
        // 最终要返回的值
        var resultMap = Map[TopicAndPartition, Long]()
        val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topics)
        topicAndPartitionEither match {
            case Right(topicAndPartitionSet) =>
                val topicAndPartitionToOffsetEither: Either[Err, Map[TopicAndPartition, Long]] = cluster.getConsumerOffsets(groupId, topicAndPartitionSet)
                topicAndPartitionToOffsetEither match {
                    // 表示拿到相应topic分区的偏移量
                    case Right(map) => resultMap ++= map
                    // 没有取到相应的偏移量, 是第一次消费. 每个分区都应该从 0开始消费
                    case Left(err) =>
                        topicAndPartitionSet.foreach(topicAndPartition => {
                            resultMap += topicAndPartition -> 0L
                        })
                }
            case _ => // 如果是Left值, 不需要做任何处理. 表示的topic和分区不存在
        }
        resultMap
    }
    // 保存消费的偏移量, 然后在读的时候才可以读到上次的位置开始接着消费
    def save(sourceStream: InputDStream[String]) = {
        // 保存偏移量, 必须每消费一次, 保存一次
        sourceStream.foreachRDD(rdd => {
            var map = Map[TopicAndPartition, Long]()
            
            // 1. 把rdd强壮成包含分区偏移量的对象.   rdd必须是从kafka直接出来的那个Stream内部的
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            val offsetRanges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
            
            offsetRanges.foreach(offsetRange => {
                val key: TopicAndPartition = offsetRange.topicAndPartition()
                val value: Long = offsetRange.untilOffset   // 0 to 10 | 0 until 10
                map += key -> value
            })
            cluster.setConsumerOffsets(groupId, map)
        })
    }
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        
        val sourceStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            params,
            readOffsets(),
            (handler: MessageAndMetadata[String, String]) => handler.message()
        )
        // 保存偏移量
        save(sourceStream)
        
        sourceStream.print(10000)
        ssc.start()
        ssc.awaitTermination()
        
    }
}
