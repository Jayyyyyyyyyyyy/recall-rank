package com.wifi.recommend

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Instance(feature: String, label: String)

object RecallRank {

  def saveFeatureByName(output: String, feature: RDD[(ArrayBuffer[Array[String]], String)]): Unit = {
    val samples = feature.map { case (f, label) =>
      val libsvmFeature = f
        .map(kv => kv(0).replaceAll(":", "_") + ":" + kv(1)).reduce(_ + " " + _)
      label + " " + libsvmFeature
    }

    samples.saveAsTextFile(output + "/samples_by_name")
  }

  def saveFeatureById(output: String, feature: RDD[(Array[Array[String]], String)], featureMap: scala.collection.Map[String, Long]): Unit = {
    val samples = feature.map { case (f, label) =>
      val libsvmFeature = f.filter(kv => featureMap.contains(kv(0)))
        .map(kv => (featureMap(kv(0)), kv(1))).sortWith(_._1 < _._1)
        .map(kv => kv._1 + ":" + kv._2).reduce(_ + " " + _)
      label + " " + libsvmFeature
    }

    samples.saveAsTextFile(output + "/samples_by_id")
  }

  def combineFeature(a: ArrayBuffer[Array[String]], b: ArrayBuffer[Array[String]]): ArrayBuffer[Array[String]] = {
    a.flatMap { i =>
      b.map { j =>
        Array("pair(" + i(0) + "," + j(0) + ")", "1")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: <input><output><feaCount> ")
      System.exit(1)
    }
    val Array(input, output, feaCount) = args
    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[Instance], classOf[String], classOf[Array[Array[String]]], classOf[Array[String]]))

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val samples = spark.read.option("mergeSchema", "true").parquet(input)
    val filteredSamples = samples.rdd.map { row =>
      val label = row.getAs[Int]("label").toString
      val feature = row.getAs[String]("feature")
      Instance(feature, label)
    }.filter { instance =>
      instance.feature.matches(".*d:retrieve:(cat|tag|subcat)_.*")
    }

    val filteredFeature = filteredSamples.map { instance =>
      var fullFeature = ArrayBuffer[Array[String]]()
      var docFeature = ArrayBuffer[Array[String]]()
      var contextFeature = ArrayBuffer[Array[String]]()
      var retrieveFeature = ArrayBuffer[Array[String]]()
      var relateFeatureMap = mutable.Map[String, String]()
      var ctrFeatureMap = mutable.Map[String, String]()
      var retrieveBias = ArrayBuffer[Array[String]]()
      var relateFeature = ArrayBuffer[Array[String]]()
      var ctrFeature = ArrayBuffer[Array[String]]()
      instance.feature.split("\t").map(_.split("=")).filter(arr => arr.length == 2).foreach { kv =>
        if (kv(0).matches("d:retrieve:(cat|tag|subcat)_.*")) {
          retrieveFeature += kv
          relateFeatureMap.put(kv(0)
            .replaceAll("d:retrieve:cat_", "d:ct:")
            .replaceAll("d:retrieve:tag_", "d:tags:")
            .replaceAll("d:retrieve:subcat_", "d:sct:")
            , kv(1))
          ctrFeatureMap.put(kv(0).replaceAll("retrieve:", "rstatctr"), kv(1))
        }
      }

      instance.feature.split("\t").map(_.split("=")).filter(arr => arr.length == 2).foreach { kv =>
        if (kv(0).matches("d:(taste|ct|sct|tags|topics|stats|ext|lbs_city|lbs_province|from|demand|rstatctr).*")) {
          var splitId = (Math.log1p(kv(1).toDouble) * 10).toInt
          if (splitId > 20) splitId = 20
          docFeature += Array(kv(0) + "#" + splitId, "1")
        }
        if (kv(0).matches("context:(hour|hourseg|dayOfWeek).*")) {
          contextFeature += kv
        }
        if (kv(0).matches("d:retrieve:(cat|tag|subcat)")) {
          retrieveBias += kv
        }
        if (relateFeatureMap.contains(kv(0))) {
          var splitId = (Math.log1p(kv(1).toDouble) * 10).toInt
          if (splitId > 20) splitId = 20
          relateFeature += Array("d:relate:" + kv(0).split(":")(1) + "#" + splitId, "1")
          relateFeature += Array("d:relate#" + splitId, "1")
        }
        if (ctrFeatureMap.contains(kv(0))) {
          var splitId = (Math.log1p(kv(1).toDouble) * 10).toInt
          if (splitId > 20) splitId = 20
          ctrFeature += Array(kv(0).split("_")(0) + "#" + splitId, "1")
          ctrFeature += Array("d:rstatctr#" + splitId, "1")
        }
      }

      val retrieveDocPair = combineFeature(retrieveFeature, docFeature)
      val contextDocPair = combineFeature(contextFeature, docFeature)

      fullFeature ++= docFeature
      fullFeature ++= retrieveFeature
      fullFeature ++= retrieveBias
      fullFeature ++= contextFeature
      fullFeature ++= relateFeature
      fullFeature ++= ctrFeature
      fullFeature ++= retrieveDocPair
      fullFeature ++= contextDocPair
      (fullFeature, instance.label)
    }
    val colNamesRaw = filteredFeature.flatMap {
      case (f, l) => f.map(_ (0))
    }.map(fea => (fea, 1)).reduceByKey(_ + _)
      .filter(_._2 >= feaCount.toInt).cache()

    //colNamesRaw.saveAsTextFile(output + "/feature_count")
    val featureMapCache = colNamesRaw.map(_._1).zipWithIndex().cache()
    featureMapCache.saveAsTextFile(output + "/feature_map")
    val featureMap = featureMapCache.collectAsMap()

    saveFeatureByName(output, filteredFeature)
    //saveFeatureById(output, filteredFeature, featureMap)
  }
}
