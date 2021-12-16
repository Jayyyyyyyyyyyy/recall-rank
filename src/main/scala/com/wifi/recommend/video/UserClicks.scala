package com.wifi.recommend.video

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.hashing.MurmurHash3
import scala.util.matching.Regex

/**
  * Created by:liujikun
  * Date: 2017/9/18
  */
object UserClicks {
  val userPattern= new Regex("(?<=\"deviceId\":\").*?(?=\")")
  val itemPattern = new Regex("(?<=\"newsid\":\").*?(?=\")")
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      Console.err.println("input, output, maxview, minview")
    }
    val Array(input, output, maxview, minview) = args
    val conf = new SparkConf().setAppName("UserClicks")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(input).coalesce(100).map { r =>
        val userId = userPattern.findFirstIn(r).getOrElse("0")
        val videoId = itemPattern.findFirstIn(r).getOrElse("0")
        (videoId,userId)
    }.filter(r=>r._1.length>20 && r._2.length>1).groupByKey()
      .map{case (i,ulist)=> (i, ulist.take(maxview.toInt))}.filter(_._2.size>=minview.toInt)
      .flatMap{
        case (i,ulist)=>
          ulist.map(u=>(u, i))
      }.groupByKey().filter(ui=>ui._2.size > 4 && ui._2.size < 500)
      .cache()

    val itemMapCache=rdd.flatMap(_._2).distinct().zipWithIndex().cache()
    itemMapCache.map(r=>r._2+"\t"+r._1).saveAsTextFile(output+"/map")
    val itemMap= itemMapCache.collectAsMap()


    rdd.flatMap{
      case (u,ilist)=>
        ilist.map(i=>MurmurHash3.stringHash(u)+"\t"+itemMap.getOrElse(i, "null")+"\t1")
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
