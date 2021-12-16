package com.wifi.recommend

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.joda.time.DateTime

import scala.util.matching.Regex
/**
  * Created by:liujikun
  * Date: 2017/9/18
  */
object StreamingClicks {
  val aidReg = new Regex("(?<=\"aid\":\").*?(?=\")")
  val idReg = new Regex("(?<=\"id\":\").*?(?=\")")
  val datatypeReg = new Regex("(?<=\"datatype\":\").*?(?=\")")
  val timestampReg = new Regex("(?<=\"timestamp\":).*?(?=,)")
  val basedir="/stream/video/click"

  def main(args: Array[String]): Unit = {
    val ckpoint = s"$basedir/checkpoint"
    val ssc = StreamingContext.getOrCreate(ckpoint, ()=>createContext(ckpoint))
    ssc.start()
    ssc.awaitTermination()

  }

  private def createContext(ckpoint: String) :StreamingContext ={
    val msgConsumerGroup = "videoclick-group"
    val conf = new SparkConf().setAppName("StreamJobDoNotKill")
    conf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    val ssc = new StreamingContext(conf, Minutes(60))
    ssc.checkpoint(ckpoint)
//    val kstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc,
//      Map("metadata.broker.list" -> "10.19.182.158:9092",
//        "groupid"->msgConsumerGroup,
//        "enable.auto.commit" -> "false"),
//      Set("video25_action_other", "video19_action_other"))


    val kstream = KafkaUtils.createStream(ssc, "10.19.88.224:2181", msgConsumerGroup,
      Map("video25_action_other" -> 64, "video19_action_other"-> 64))

    kstream.map(_._2).filter(_.contains("\"action\":\"Click\"")).map{line=>
      val aid = aidReg.findFirstIn(line).getOrElse("null")
      val id = idReg.findFirstIn(line).getOrElse("null~null").split("~")(1)
      val datatype = datatypeReg.findFirstIn(line).getOrElse("null")
      val timestamp = timestampReg.findFirstIn(line).getOrElse("null")
      Array(aid,id,datatype,timestamp).mkString("\t")
    }.foreachRDD { (rdd, time) =>
      val hour=new DateTime(time.milliseconds).minusHours(1).toString("yyyyMMddHH")
      val path=s"$basedir/dt=$hour"
      rdd.coalesce(5).saveAsTextFile(path)
    }
    ssc
  }
}
