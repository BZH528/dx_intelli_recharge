package com.gy.hcharge

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by Administrator on 2019/11/14 0014.
  */
object Util {
  // 计算当前时间距离次日零点的时长（毫秒）
  def resetTime = {
    val now = new Date()
    val todayEnd = Calendar.getInstance
    todayEnd.set(Calendar.HOUR_OF_DAY, 23) // Calendar.HOUR 12小时制
    todayEnd.set(Calendar.MINUTE, 59)
    todayEnd.set(Calendar.SECOND, 59)
    todayEnd.set(Calendar.MILLISECOND, 999)
    todayEnd.getTimeInMillis - now.getTime
  }

  def getDate():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    var now=dateFormat.format(cal.getTime())
    now
  }

  def getTomorrow():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,1)
    var tomorrow=dateFormat.format(cal.getTime())
    tomorrow
  }


  def getDayStartTime():String={
    val tmpStr = getDate()+" 00:00:00"
    val start = Timestamp.valueOf(tmpStr).getTime
    String.valueOf(start)
  }
  def getDayEndTime():String={
    val tmpStr = getTomorrow()+" 00:00:00"
    val start = Timestamp.valueOf(tmpStr).getTime
    String.valueOf(start)
  }



  def NowTime(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }
  def getFormatTime:String={
    val now = NowTime()
    val tmp = now.split(":")
    val second = Integer.valueOf(tmp(2))
    var formatSecond="00"
    if(second>=0 && second-40<0 && second-20<0){
      formatSecond="00"
    }
    else if(second>=0 && second-20>=0&& second-40<0){
      formatSecond = "20"
    }
    else{
      formatSecond ="40"
    }

    return tmp(0)++":"+tmp(1)+":"+formatSecond
  }
}
