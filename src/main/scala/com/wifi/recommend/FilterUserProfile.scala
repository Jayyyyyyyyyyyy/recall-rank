package com.wifi.recommend

import breeze.linalg._
import org.apache.spark
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

class Prof(line: String) {
  private val Array(_userId, _cs, _tag, _weight, a, b, c, _oldUser) = line.split("\t")
  val userId: String = _userId
  val cs: String = _cs
  val tag: String = _tag
  val weight: String = _weight
  val oldUser: String = _oldUser
}

object FilterUserProfile {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      Console.err.println("profile, onlyOld, output")
    }
    val Array(profile, onlyOld, output) = args
    val conf = new SparkConf().setAppName("UserLikes")
    conf.set("spark.hadoop.io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")

    val sc = new SparkContext(conf)

    sc.textFile(profile).coalesce(500).map { line =>
      new Prof(line)
    }.filter { prof =>
      if (onlyOld.equals("1")) {
        prof.cs.equals("tags_cs") && prof.oldUser.equals("1")
      } else {
        prof.cs.equals("tags_cs")
      }
    }.map { prof =>
      prof.userId + "\t" + prof.tag + ":" + prof.weight
    }.saveAsTextFile(output)
  }
}
