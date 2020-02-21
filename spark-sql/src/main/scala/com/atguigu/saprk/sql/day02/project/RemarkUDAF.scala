package com.atguigu.saprk.sql.day02.project

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object RemarkUDAF extends UserDefinedAggregateFunction {
    // 北京 天津 字符串类型
    override def inputSchema: StructType = StructType(StructField("city_name", StringType) :: Nil)
    
    // 缓冲区的数据  北京->1000 天津->800   总的点击数
    override def bufferSchema: StructType = {
        StructType(StructField("map", MapType(StringType, LongType)) :: StructField("total", LongType) :: Nil)
    }
    
    // 返回值的类型 字符串
    override def dataType: DataType = StringType
    
    override def deterministic: Boolean = true
    
    //
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Map[String, Long]()
        buffer(1) = 0L
    }
    
    // 分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 北京  天津
        if (!input.isNullAt(0)) {
            val cityName: String = input.getString(0)
            // 从map取出城市对应的点击量. 然后+1, 再新的值存入到map中
            val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
            val preCount: Long = map.getOrElse(cityName, 0L) // 获取旧值
            buffer(0) = map + (cityName -> (preCount + 1L)) // 注意map的不不可变性
            
            // 更新总数
            buffer(1) = buffer.getLong(1) + 1L
        }
    }
    
    // 分区间的聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        // 总数的合并
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
        // map的合并
        val map1 = buffer1.getMap[String, Long](0)
        val map2 = buffer2.getMap[String, Long](0)
        
        buffer1(0) = map1.foldLeft(map2) {
            case (map, (cityName, count)) =>
                map + (cityName -> (map.getOrElse(cityName, 0L) + count))
        }
    }
    
    // 返回最终的值  北京21.2%，天津13.2%，其他65.6%
    override def evaluate(buffer: Row): Any = {
        val cityCount: collection.Map[String, Long] = buffer.getMap[String, Long](0)
        val total = buffer.getLong(1)
        
        // 按照点击量进行排序 , 取top2
        val cityCountTop2: List[(String, Long)] = cityCount.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)
        var remarkList = cityCountTop2.map {
            case (city, count) => Remark(city, count.toDouble / total)
        }
        remarkList = remarkList :+ Remark("其他", remarkList.foldLeft(1D)(_ - _.cityRatio))
        remarkList.mkString(", ")
    }
}
// 0.12346  12.35%
case class Remark(city: String, cityRatio: Double){
    val f = new DecimalFormat(".00%")
    
    override def toString: String = s"$city:${f.format(cityRatio)}"
}