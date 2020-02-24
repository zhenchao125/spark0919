package com.atguigu.spark.steaming.project.util

import redis.clients.jedis.Jedis

/**
  * Author atguigu
  * Date 2020/2/24 16:19
  */
object RedisUtil {
    /*
    两种方式:
     1. 使用连接池
            效率更高, 连接会重用
            实际情况, 容易出现多线程 bug
     
     2. 手动创建连接的客户端对象
            用完务必关闭!
     */
    val host = "hadoop102"
    val port = 6379
    
    // 获取一个客户端对象
    def getClient = new Jedis(host, port)
    
    // 关闭客户端
    def close(client: Jedis) = {
        if(client != null) client.close()
    }
}
