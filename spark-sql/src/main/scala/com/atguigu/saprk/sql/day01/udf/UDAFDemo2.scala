package com.atguigu.saprk.sql.day01.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Author atguigu
  * Date 2020/2/19 14:23
  */
object UDAFDemo2 {
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
        spark.udf.register("my_avg", new MyAvg)
        val df = spark.read.json("c:/users.json")
        df.createOrReplaceTempView("user")
        spark.sql("select my_avg(age) from user").show
        
        spark.close()
        
    }
}

class MyAvg extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField("ele", DoubleType)::Nil)
    
    override def bufferSchema: StructType = StructType(StructField("sum", DoubleType)::StructField("c", IntegerType)::Nil)
    
    override def dataType: DataType = DoubleType
    
    override def deterministic: Boolean = true
    
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0d
        buffer(1) = 0
        
    }
    
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!input.isNullAt(0)) {
            val value: Double = input.getDouble(0)
            buffer(0) = buffer.getDouble(0) + value
            buffer(1) = buffer.getInt(1) + 1
        }
    }
    
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val sum1: Double = buffer1.getAs[Double](0)
        val count1: Int = buffer1.getInt(1)
        
        val sum2: Double = buffer2.getDouble(0)
        val count2: Int = buffer2.getInt(1)
        
        buffer1(0) = sum1 + sum2
        buffer1(1) = count1 + count2
    }
    
    override def evaluate(buffer: Row): Any = buffer.getDouble(0) / buffer.getInt(1)
}
