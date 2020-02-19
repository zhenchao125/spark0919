package com.atguigu.saprk.sql.day01.rdddfds

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author atguigu
  * Date 2020/2/19 13:43
  */
object CreateDFLByApi {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("CreateDFLByApi")
            .getOrCreate()
        import spark.implicits._
        
        //通过api代码吧rdd转换成df
        val list1 = List(("lisi", 10), ("zs", 20), ("zhiling", 40))
        val listRDD: RDD[(String, Int)] = spark.sparkContext.parallelize(list1)
        val rowRDD: RDD[Row] = listRDD.map {
            case (name, age) =>
                Row(name, age)
        }
        
//        val schema: StructType = StructType(List(StructField("name1", StringType), StructField("age1", IntegerType)))
        val schema: StructType = StructType(StructField("name1", StringType)::StructField("age1", IntegerType)::Nil)
        
        val df: DataFrame = spark.createDataFrame(rowRDD, schema)
        df.show(1000)
        spark.close()
        
        
    }
}
