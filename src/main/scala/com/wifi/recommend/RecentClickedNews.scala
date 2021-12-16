package com.wifi.recommend

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.hashing.MurmurHash3
import scala.util.matching.Regex

/**
  * Created by:liujikun
  * Date: 2017/9/18
  */
object RecentClickedNews {
  val userPattern= new Regex("(?<=\"deviceId\":\").*?(?=\")")
  val newsPattern = new Regex("(?<=\"newsid\":\")\\d+(?=\")")
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      Console.err.println("input, output, maxview, minview")
    }
    val Array(input, output, maxview, minview) = args
    val conf = new SparkConf().setAppName("UserClicks")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(input).map { r =>
        val userId = userPattern.findFirstIn(r).getOrElse("0")
        val newsId = newsPattern.findFirstIn(r).getOrElse("0")
        (userId,newsId)
    }.filter(ids=>ids._1.length>10 && ids._2.length>10)
    .map { r =>
      val hashedUserId = MurmurHash3.stringHash(r._1)
      val newsId = r._2
      (newsId, hashedUserId)
    }.groupByKey().map{case (i,ulist)=> (i, ulist.take(maxview.toInt))}.filter(_._2.size>=minview.toInt)
      .keys.saveAsTextFile(output+"/newsfilter")
  }

}
