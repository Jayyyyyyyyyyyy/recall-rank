package com.wifi.recommend

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object FilterNewsLikes {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      Console.err.println("input, output")
    }
    val Array(input, output) = args
    val conf = new SparkConf().setAppName("FilterNewsLikes")
    conf.set("spark.hadoop.io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")
    val sc = new SparkContext(conf)

    val userLikes = sc.textFile(input).coalesce(500).mapPartitions { lines =>
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      lines.map { l =>
        try {
          val json = mapper.readValue[Map[String, Object]](l)
          if (json.contains("tagsMap") &&
          json("expire").asInstanceOf[Long] > System.currentTimeMillis &&
            json("is_mask").asInstanceOf[Int]==0) {
            val tagsMap=json("tagsMap").asInstanceOf[Map[String, Double]]

            val id = if(json.contains("id")) {
              json("id").asInstanceOf[String]
            } else {
              json("_id").asInstanceOf[String]
            }
            (id, tagsMap)
          } else
            ("unknown_id", Map("NONE" -> 0.0))
        } catch {
          case _: Throwable => ("except_id", Map("NONE" -> 0.0))
        }
      }.filter(_._2.size>1)
    }.reduceByKey((x,y)=>x).persist(StorageLevel.DISK_ONLY)
    userLikes.map(l=>l._1+"\t"+l._2.map(x => x._1 + ":" + x._2).reduce(_ + " " + _)).coalesce(100)
      .saveAsTextFile(output + "/likes")
  }

}
