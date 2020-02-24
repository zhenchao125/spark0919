package com.atguigu.spark.steaming.project.bean

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(ts: Long,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String,
                   var timestamp: Timestamp = null,
                   var dayString: String = null, // 2019-12-18
                   var hmString: String = null) { // 10:20{
    
    timestamp = if (timestamp == null) new Timestamp(ts) else timestamp
    val date = new Date(ts)
    dayString = if (dayString == null) new SimpleDateFormat("yyyy-MM-dd").format(date) else dayString
    hmString = if (hmString == null) new SimpleDateFormat("HH:mm").format(date) else hmString
}
