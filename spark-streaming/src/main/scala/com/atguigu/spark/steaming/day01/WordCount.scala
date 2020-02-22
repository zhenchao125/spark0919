package com.atguigu.spark.steaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/22 10:00
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        // 1.创建SteamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        // 2. 从数据源读取数据, 得到 DSteam
        val sourceSteam: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        // 3. 对DStream左各种转换得到word-count
        val wordCount: DStream[(String, Int)] = sourceSteam.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        // 4. 打印流
        wordCount.print(1000)
        // 5. 启动流
        ssc.start()
        // 6. 阻止主线程退出
        ssc.awaitTermination()
    }
}
