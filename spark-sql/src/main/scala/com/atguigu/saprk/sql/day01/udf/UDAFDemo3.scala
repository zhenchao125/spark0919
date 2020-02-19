package com.atguigu.saprk.sql.day01.udf

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
  * Author atguigu
  * Date 2020/2/19 14:23
  */
object UDAFDemo3 {
    def main(args: Array[String]): Unit = {
        
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("UDAFDemo1")
            .getOrCreate()
        import spark.implicits._
        // 要注册使用自定义函数
        val df = spark.read.json("c:/users.json")
        val userDS: Dataset[User] = df.as[User]
        
        val column: TypedColumn[User, Double] = MyAvg2.toColumn.name("avg")
        val ds: Dataset[Double] = userDS.select(column)
        ds.show(1000)
        
        spark.close()
        
    }
}

case class User(age: Long, name: String)

case class AvgAge(var ageSum: Long, var ageCount: Int) {
    def getAvg = ageSum.toDouble / ageCount
}

object MyAvg2 extends Aggregator[User, AvgAge, Double] {
    
    // 初始化缓冲区
    override def zero: AvgAge = AvgAge(0, 0)
    
    // 分区内聚合
    override def reduce(b: AvgAge, a: User): AvgAge = {
        /*b.ageSum += a.age
        b.ageCount += 1
        b*/
        AvgAge(b.ageSum + a.age, b.ageCount + 1)
    }
    
    // 分区间聚合
    override def merge(b1: AvgAge, b2: AvgAge): AvgAge = {
        AvgAge(b1.ageSum + b2.ageSum, b1.ageCount + b2.ageCount)
    }
    
    // 返回最后聚合的值
    //    override def finish(reduction: AvgAge): Double = reduction.ageSum.toDouble / reduction.ageCount
    override def finish(reduction: AvgAge): Double = reduction.getAvg
    
    // 输出缓冲区的编码器
    override def bufferEncoder: Encoder[AvgAge] = Encoders.product
    
    // 返回输出值的编码器
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
