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


object RelevantFeatureExtract {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      Console.err.println("featurePath, showPath, clickPath, featureTopN, output")
    }
    val Array(featurePath, showPath, clickPath, featureTopN, output) = args
    val conf = new SparkConf().setAppName("RelevantFeatureExtract")
    conf.set("spark.hadoop.io.compression.codecs",
      "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")
    val sc = new SparkContext(conf)

    val feature = processFeature(featurePath, sc, args)
    val show = processClickOrShow(showPath, sc)
    val click = processClickOrShow(clickPath, sc)

    show.leftOuterJoin(click).map{line=>
      if(line._2._2.isDefined){
        (line._1, 1)
      } else{
        (line._1, 0)
      }
    }.join(feature).map{line=>
      line._2._1+" "+line._2._2.map(kv=>kv._1+":"+kv._2).reduce(_+" "+_)
    }.saveAsTextFile(output+"/sample")
  }


  private def processClickOrShow(showPath: String, sc: SparkContext) = {
    sc.textFile(showPath).coalesce(100).mapPartitions { part =>
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      part.map { line =>
        val json = mapper.readValue[Map[String, String]](line)
        val newsid = json.getOrElse("newsid", "0")
        val token = json.getOrElse("token","0")
        val whr = json.getOrElse("where", "unkown")
        val tpe = json.getOrElse("type", "0")
        (((token, newsid), 1), whr, tpe)
      }.filter(v => !v._3.equals("31") && v._2.equals("detail")).map(_._1)
    }
  }

  private def processFeature(featurePath:String, sc:SparkContext, args: Array[String]) = {
    val Array(featurePath, showPath, clickPath, featureTopN, output) = args
    val feature=sc.textFile(featurePath).coalesce(100).mapPartitions { part =>
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      part.map { line =>
        val json = mapper.readValue[Map[String, Object]](line)
        val features = json("features").asInstanceOf[Map[String, Double]]
        val token = json("token").asInstanceOf[String]
        val docId = json("docId").asInstanceOf[String]
        ((token, docId), features.filter(kv => !kv._1.contains("u:") && !kv._1.contains("retrieve") && !kv._1.contains("ud")) )
      }
    }

    val featureList = feature.flatMap { line =>
      line._2.keys //get feature key
    }.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).zipWithIndex()
      .take(featureTopN.toInt).map(kv=>(kv._1._1, (kv._2, kv._1._2)))//(feature,(index, featureCount))

    sc.parallelize(featureList.toSeq).map(r=>r._2._1+"\t"+r._1.replaceAll("\\s", "")+"\tq").saveAsTextFile(output+"/feamap.txt")
    val featureMap = featureList.toMap
    feature.map{case ((token, docId),features)=>
      ((token, docId),features.filter(kv=>featureMap.contains(kv._1)))
      var indexedFeature = Array[(Long, Double)]()
      features.foreach{feature=>
        if(featureMap.contains(feature._1)){
          indexedFeature:+=(featureMap(feature._1)._1, feature._2)
        }
      }
      ((token, docId),indexedFeature.sorted)
    }
  }
}
