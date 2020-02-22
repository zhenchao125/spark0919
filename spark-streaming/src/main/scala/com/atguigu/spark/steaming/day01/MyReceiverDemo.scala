package com.atguigu.spark.steaming.day01

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/2/22 10:45
  */
object MyReceiverDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyReceiverDemo")
        val ssc = new StreamingContext(conf, Seconds(3))
        val stream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 10000))
        stream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
从sock读数据
 */
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    var socket: Socket = _
    
    var reader: BufferedReader = _
    
    // 起一个线程, 去接受数据. 务必不要阻塞这个方法
    override def onStart(): Unit = {
        runInThread {
            try {
                // 从socket读取数据
                socket = new Socket(host, port)
                // 以流的方式获取数据(java中的io流)
                val is: InputStream = socket.getInputStream
                // 字节流转换成字符流 // 封装成缓冲流
                reader = new BufferedReader(new InputStreamReader(is, "utf8"))
                var line: String = reader.readLine()
                // 当对方的流已经关闭, 则会读到一个null
                while (line != null && socket.isConnected) {
                    store(line)
                    line = reader.readLine() // 如果没有数据, 这个函数会阻塞
                    println("阻塞结束....")
                }
            } catch {
                case e => e.printStackTrace()
            } finally {
                restart("正在重启 receiver")
            }
        }
    }
    
    // 完全的停止 Receiver, 用来去释放资源
    override def onStop(): Unit = {
        if (reader != null) reader.close()
        if (socket != null) socket.close()
    }
    
    // 在一个子线程中执行 代码  op
    def runInThread(op: => Unit): Unit = {
        new Thread() {
            override def run(): Unit = op
        }.start()
    }
}

