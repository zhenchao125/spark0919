package com.atguigu.spark.steaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Author atguigu
  * Date 2020/2/22 10:32
  */
object WordCount2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount2")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val queue = mutable.Queue[RDD[Int]]()
        val stream: InputDStream[Int] = ssc.queueStream(queue, false)
        stream.reduce(_ + _).print(100)
        
        ssc.start()
    
        while (true) {
            println(queue.size)
            val rdd = ssc.sparkContext.parallelize(1 to 100)
            queue.enqueue(rdd)
            Thread.sleep(2000)
        }
        
        ssc.awaitTermination()
        
        
    }
}
