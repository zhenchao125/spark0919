package com.atguigu.saprk.sql.day01.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

/**
  * Author atguigu
  * Date 2020/2/19 14:23
  */
object UDAFDemo1 {
    def main(args: Array[String]): Unit = {
        /*
        自定义聚合函数, 两种:
        1. 一种弱类型
            用在sql语句中
        2. 一种强类型
            用在dataset的dsl风格的编程的使用
         */
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("UDAFDemo1")
            .getOrCreate()
        import spark.implicits._
        // 要注册使用自定义函数
        spark.udf.register("my_sum", new MySum)
        val df = spark.read.json("c:/users.json")
        df.createOrReplaceTempView("user")
        spark.sql("select my_sum(age) from user").show
        
        spark.close()
        
        
        
        
    }
}
/*
name: "aaa" age: 10
name: "bbb"

select mysum(age) from user group by ..

计算平均值的聚合函数;  avg
sum = 0
sum += 1
sum += 2
 */
class MySum extends UserDefinedAggregateFunction {
    // 表示传递给聚合函数的参数的类型
    override def inputSchema: StructType = StructType(StructField("ele", DoubleType)::Nil)
    
    // 缓冲的数据类型
    override def bufferSchema: StructType = StructType(StructField("sum", DoubleType)::Nil)
    
    // 最终结果的类型
    override def dataType: DataType = DoubleType
    
    // 相同输入是否应该返回相同的输出  确定性
    override def deterministic: Boolean = true
    
    // 初始化: 对缓冲区进行初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0d
    }
    
    // 分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 把传来的值取出, 和缓冲区的值相加, 然后把结果再存入到缓冲区, 下一行计算继续通用的动作
        if (!input.isNullAt(0)) { // 对传入的数据是否为null做判断, 防止空指针
            val value: Double = input.getDouble(0)
            buffer(0) = buffer.getDouble(0) + value
        }
    }
    
    // 分区间聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//        val v1: Double = buffer1.getDouble(0)
        val v1: Double = buffer1.getAs[Double](0)  // 等价于上一行
        val v2: Double = buffer2.getDouble(0)
        
        buffer1(0) = v1 + v2
    }
    
    // 返回聚合后的最终的值
    override def evaluate(buffer: Row): Any = buffer.getDouble(0)  // buffer(0)
}
