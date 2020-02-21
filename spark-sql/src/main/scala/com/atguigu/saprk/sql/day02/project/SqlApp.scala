package com.atguigu.saprk.sql.day02.project

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/2/21 14:04
  */
object SqlApp {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SqlApp")
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        spark.udf.register("remark", RemarkUDAF);
        spark.sql("use sql0919")
        // 1. join表, 然后把需要字段查询出来   t1
        spark.sql(
            """
              |select
              |    ci.city_name,
              |    ci.area,
              |    uv.click_product_id,
              |    pi.product_name
              |from user_visit_action uv
              |join product_info pi on uv.click_product_id=pi.product_id
              |join city_info ci on uv.city_id=ci.city_id
            """.stripMargin).createOrReplaceTempView("t1")
        // 2. 按照地区和产品的名字做聚合  t2
        
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    count(*) ct,
              |    remark(city_name) remark
              |from t1
              |group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")
        // 3. 每个地区的商品的点击量排序 开窗   t3   1000 800 800 700
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    ct,
              |    remark,
              |    rank() over(partition by area order by ct desc) rk
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")
        // 4.每个地区取前3
    
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val tbtale = "area_click_count_top"
        val props = new Properties()
        props.put("user", "root")
        props.put("password", "aaaaaa")
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    ct,
              |    remark
              |from t3
              |where rk<=3
            """.stripMargin).write.jdbc(url, tbtale, props)
        
        spark.close()
    }
}
/*

1. join表, 然后把需要字段查询出来   t1

select
    ci.city_name,
    ci.area,
    uv.click_product_id,
    pi.product_name
from user_visit_action uv
join product_info pi on uv.click_product_id=pi.product_id
join city_info ci on uv.city_id=ci.city_id

2. 按照地区和产品的名字做聚合  t2
select
    area,
    product_name,
    count(*) ct
from t1
group by area, product_name

3. 每个地区的商品的点击量排序 开窗   t3   1000 800 800 700
select
    area,
    product_name,
    ct,
    rank() over(partition by area order by ct desc) rk     // rank row_number dense_rank
form t2

4. 每个地区取前3
select
    area,
    product_name,
    ct
from t3
where rk<=3




CREATE TABLE `user_visit_action`(
  `date` string,
  `user_id` bigint,
  `session_id` string,
  `page_id` bigint,
  `action_time` string,
  `search_keyword` string,
  `click_category_id` bigint,
  `click_product_id` bigint,
  `order_category_ids` string,
  `order_product_ids` string,
  `pay_category_ids` string,
  `pay_product_ids` string,
  `city_id` bigint)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/user_visit_action.txt' into table user_visit_action;

CREATE TABLE `product_info`(
  `product_id` bigint,
  `product_name` string,
  `extend_info` string)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/product_info.txt' into table product_info;

CREATE TABLE `city_info`(
  `city_id` bigint,
  `city_name` string,
  `area` string)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/city_info.txt' into table  city_info;


 */