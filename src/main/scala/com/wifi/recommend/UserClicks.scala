package com.wifi.recommend

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.hashing.MurmurHash3
import scala.util.matching.Regex

/**
  * Created by:liujikun
  * Date: 2017/9/18
  */
object UserClicks {
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
        val userId = r._1
        val newsId = r._2
        (newsId, userId)
      }.groupByKey().map{case (i,ulist)=> (i, ulist.take(maxview.toInt))}.filter(_._2.size>=minview.toInt)
      .flatMap{
        case (i,ulist)=>
          ulist.map(u=>(u, i))
      }.groupByKey().filter(ui=>ui._2.size > 1 && ui._2.size < 200)
      .cache()

    rdd.flatMap{
      case (u,ilist)=>
        ilist.map(i=>MurmurHash3.stringHash(u)+"\t"+i+"\t1")
    }.saveAsTextFile(output+"/train")
    rdd.map{
      case (u,ilist)=>
        u+"\t"+ilist.reduce(_+" "+_)
    }.saveAsTextFile(output+"/list")
    rdd.map { r =>
      val userId=r._1
      val hashedId = MurmurHash3.stringHash(userId)
      hashedId+"\t"+userId
    }.distinct().saveAsTextFile(output+"/hash")
  }

}
