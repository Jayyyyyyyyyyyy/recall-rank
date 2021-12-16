package com.wifi.recommend

import breeze.linalg._

import scala.util.matching.Regex
import org.apache.spark
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.rdd.RDD

import scala.collection.mutable


object RelevantPairCtr {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      Console.err.println("showPath, clickPath, topN, output")
    }
    val Array(showPath, clickPath, topN, output) = args
    val conf = new SparkConf()
    conf.set("spark.hadoop.io.compression.codecs",
      "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")
    val sc = new SparkContext(conf)

    val show = processClickOrShow(showPath, sc)
    val click = processClickOrShow(clickPath, sc)

    click.join(show).map{line=>
        (line._1, line._2._1/(line._2._2+1).toDouble)
    }.map{case((nid,rid),ctr)=>
      (nid,(rid,ctr))
    }.groupByKey().map{case (nid, ridctrList)=>
      val recList = ridctrList.take(topN.toInt).toArray.sortWith(_._2 > _._2)
        .map(r => r._1 + ":" + "%5f".format(r._2)).reduce(_ + " " + _)
      "pairctr_"+nid+"\t"+recList
    }.saveAsTextFile(output)
  }


  private def processClickOrShow(showPath: String, sc: SparkContext) = {
    sc.textFile(showPath).coalesce(100).mapPartitions { part =>
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      part.map { line =>
        val json = mapper.readValue[Map[String, String]](line)
        val rid = json.getOrElse("newsid", "0")
        val nid = json.getOrElse("token","0").split("-")(0)
        val whr = json.getOrElse("where", "unkown")
        (((nid, rid), 1), whr)
      }.filter(v=>v._2.equals("detail")).map(_._1)
    }.reduceByKey(_+_)
  }

}
