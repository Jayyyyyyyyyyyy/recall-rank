package com.wifi.recommend

import breeze.linalg._
import scala.util.matching.Regex
import org.apache.spark
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object FilterUserLikes {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      Console.err.println("profile, clicks, output, media(news|video)")
    }
    val Array(profile, clicks, output, media) = args
    val conf = new SparkConf().setAppName("UserLikes")
    conf.set("spark.hadoop.io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")
    val deviceId = new Regex("(?<=\"deviceId\":\").*?(?=\")")
    val newsid = new Regex("(?<=\"newsid\":\").*?(?=\")")
    val sc = new SparkContext(conf)
    val usersMap = sc.textFile(clicks).filter{line=>
      val newsId = newsid.findFirstIn(line).getOrElse("0")
      if(media.equals("video") && newsId.length>20){
        true
      } else if(media.equals("news") && newsId.length==16){
        true
      } else{
        false
      }
    }.map { line =>
      var user = deviceId.findFirstIn(line).getOrElse("unknow_deviceId")
      (user, 1)
    }.distinct().collectAsMap()
    val buserMap = sc.broadcast(usersMap)
    val profileRdd = sc.textFile(profile).coalesce(200)
      profileRdd.filter(l => buserMap.value.contains(l.split("\t")(0)))
      .map { l =>
        (l.split("\t")(0), l.split("\t")(1))
      }.reduceByKey(_ + " " + _).map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(output + "/likes")
  }
}
