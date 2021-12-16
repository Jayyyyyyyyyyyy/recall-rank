package com.wifi.recommend.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ListBuffer


object DataUtils {
  val now = new Date()
  //获取某一天前 N 天日期
  def getPreviousdate(date: String, pre_cnt:Int):String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar = Calendar.getInstance()
    var dt:Date = dateFormat.parse(date)
    cal.setTime(dt);
    cal.add(Calendar.DATE, -pre_cnt)
    var pre_date = dateFormat.format(cal.getTime())
    return pre_date
  }
  //获取某一天后 N 天日期
  def getAfterdate(date: String, after_cnt:Int):String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar = Calendar.getInstance()
    var dt:Date = dateFormat.parse(date)
    cal.setTime(dt);
    cal.add(Calendar.DATE, after_cnt)
    var pre_date = dateFormat.format(cal.getTime())
    return pre_date
  }

  //获取某一天前 N 天日期
  def getPreviousdate(date: String, pre_cnt:Int, patten:String):String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat(patten)
    var cal:Calendar = Calendar.getInstance()
    var dt:Date = dateFormat.parse(date)
    cal.setTime(dt);
    cal.add(Calendar.DATE, -pre_cnt)
    var pre_date = dateFormat.format(cal.getTime())
    return pre_date
  }
  //获取某一天后 N 天日期
  def getAfterdate(date: String, after_cnt:Int, patten:String):String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat(patten)
    var cal:Calendar = Calendar.getInstance()
    var dt:Date = dateFormat.parse(date)
    cal.setTime(dt);
    cal.add(Calendar.DATE, after_cnt)
    var pre_date = dateFormat.format(cal.getTime())
    return pre_date
  }
  //获取两个日期的间隔天数
  def getDateDiff(date1: String, date2: String): Int = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal_1:Calendar = Calendar.getInstance()
    var cal_2:Calendar = Calendar.getInstance()
    var dt_1:Date = dateFormat.parse(date1)
    var dt_2:Date = dateFormat.parse(date2)
    cal_1.setTime(dt_1)
    var time_1 = cal_1.getTimeInMillis()
    cal_2.setTime(dt_2)
    var time_2 = cal_2.getTimeInMillis()
    var diff_days=(time_1 - time_2) / (1000*3600*24)
    return diff_days.toInt.abs
  }
  //时间转时间戳
  def transformTime(date1: String): Long = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal_1:Calendar = Calendar.getInstance()
    var dt_1:Date = dateFormat.parse(date1)
    cal_1.setTime(dt_1)
    var time_1: Long = cal_1.getTimeInMillis()
    return time_1
  }
  def getCurrent_time(): Long = {
    val a = now.getTime
    var str = a+""
    str.substring(0,10).toLong
  }
  def getZero_time():Long={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    var str = a+""
    str.substring(0,10).toLong
  }
  def timeFormat(time:String):Long={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val a = dateFormat.parse(time).getTime
    var str = a+""
    str.substring(0,10).toLong
  }

  def timeFormatUTC(time:String):Long={
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    val time = sdf.format(new Date());
    //解析时间 2016-01-05T15:09:54Z
    val date = sdf.parse(time).getTime;
    var str = date+""
    str.substring(0,10).toLong
  }
  def getDayTimeUTC(time:String):String={
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    val date = sdf.parse(time);
    val calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8);
    //calendar.getTime() 返回的是Date类型，也可以使用calendar.getTimeInMillis()获取时间戳
    //System.out.println("北京时间: " + calendar.getTime().toString);
    val format0 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val format1: String = format0.format(calendar.getTime())
    //System.out.println("0: " + format1.substring(0,10));
    format1.substring(0,10)
  }
  def getInDateBefore(inTime:String,num:Int):String={
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(inTime);
    val calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.DAY_OF_MONTH, -1*num)
    sdf.format(calendar.getTime())
  }

  def main(args: Array[String]): Unit = {
    println(transformTime("2020-10-27 00:00:00"))
    println(transformTime("2020-10-27 09:03:20.000"))
    println(transformTime("2020-10-28 00:00:00"))
//    1603209600000  1603296000000
//      1603209600000 1603296000000
                    //1603760600000
    println("============================="+getAfterdate("20201027",-1))

    val list = new ListBuffer[String]
    val t2 = 1 to Integer.valueOf("5")
    val oneDayAgo = "2020-10-27"
    for(dis <- t2){
      val newtime = getAfterdate(oneDayAgo,dis,"yyyy-MM-dd")
      list.append(newtime)
      println(newtime)
    }

  }

}

