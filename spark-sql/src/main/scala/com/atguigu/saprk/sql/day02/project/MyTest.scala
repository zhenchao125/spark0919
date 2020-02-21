package com.atguigu.saprk.sql.day02.project

import java.text.DecimalFormat

/**
  * Author atguigu
  * Date 2020/2/21 15:47
  */
object MyTest {
    def main(args: Array[String]): Unit = {
        
        val f = new DecimalFormat(".00%")
        println(f.format(10.23115))
//        println(f.format(1.235))
//        println(f.format(12.235))
//        println(f.format(234.235))
    }
}
