import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/2/28 14:42
  */
object MapJson {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapJson").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List((30, 1),(30, 2),(40, 1),(50, 1), (60, 1))
        val list2 = List((30, "a"),(30, "b"),(40, "c"),(50, "d"), (70, "e"))
        
        val rdd1: RDD[(Int, Int)] = sc.parallelize(list1)
        val rdd2= sc.parallelize(list2)
        
//        val rdd3: RDD[(Int, (Int, String))] = rdd1.join(rdd2)
//        rdd3.collect.foreach(println)
        // map端join:
        // 1. 把数据较小的那个RDD广播
        val bd: Broadcast[Array[(Int, String)]] = sc.broadcast(rdd2.collect())
        // 2. 对数据比较多的那个RDD做map操作
        val rdd4 = rdd1.flatMap{
            case (k1, v1) =>
                val r2 = bd.value
                r2.filter(_._1 == k1).map{
                    case (k2, v2) => (k1, (v1, v2))
                }
        }
        rdd4.collect.foreach(println)
        sc.stop()
        
    }
}
/*
(30, (1, a))
(30, (1, b))
(30, (2, a))
(30, (2, b))
(40, (1, c))
(50, (1, d))

------

 (30,(1,a))
(30,(1,b))
(30,(2,a))
(30,(2,b))
(40,(1,c))
(50,(1,d))


 */