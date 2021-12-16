package com.wifi.recommend

import java.util.NoSuchElementException

import breeze.linalg._
import breeze.linalg.functions.cosineDistance
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object UserVectors {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      Console.err.println("vocab, model, likes, mintagnums, dim, output")
    }

    val Array(vocab, model, likes, mintagnums, dim, output) = args
    val conf = new SparkConf().setAppName("UserVectors")
    conf.set("spark.hadoop.io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")
    //conf.set("spark.hadoop.io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec")

    val sc = new SparkContext(conf)
    val vocabMap = sc.textFile(vocab).map(_.replaceAll("\\(|\\)", "").split(",")).map(x => (x(1), x(0))).collectAsMap()
    val modelMap = sc.textFile(model).map(_.split("\t")).map(x => (vocabMap(x(0)), x(1).split(" ").map(_.toDouble))).collectAsMap()
    val likesRdd = sc.textFile(likes).filter(x => x.split("\t").length==2).map(x => x.split("\t"))
      .filter(_ (1).split(" ").length >= mintagnums.toInt)
      .map { case Array(user, like) =>
        val weightSum = like.split(" ").filter(_.split(":").length == 2).map { x =>
          if (modelMap.contains(x.split(":")(0))) {
            x.split(":")(1).toDouble
          } else {
            0.0
          }
        }.sum
        val userVector = like.split(" ").filter(_.split(":").length == 2)
          .map { tagWeight =>
            val Array(tag, weight) = tagWeight.split(":")
            val tagVector = modelMap.getOrElse(tag, Array.fill[Double](dim.toInt)(0))
            DenseVector(tagVector).copy :*= (weight.toDouble / weightSum)
        }.reduce(_ + _)
        (user, userVector, like)
      }
    likesRdd.map(x => x._1 + "\t" + x._2.map("%5f".format(_)).reduce(_ + " " + _))
      .coalesce(500).saveAsTextFile(output + "/vector")

//    val users=sc.textFile("/liujikun/vector_cs/2017111019/vector/part-00000").map(x=>(x.split("\t")(0),DenseVector(x.split("\t")(1).split(" ").map(_.toDouble)).copy))
//    val samp = users.take(20).flatMap { case (u,v) =>
//      likesRdd.map(l => ((l._1, l._3), cosineDistance(l._2, v)))
//        .filter(_._2 > 0.0).sortBy(_._2).take(50) :+ ((0L, DenseVector(1.0), "----- "+u+" ------"), 0.0) :+ ((0L, DenseVector(1.0), "=====split line====="), 0.0)
//    }
//    sc.parallelize(samp, 100).saveAsTextFile(output + "/test")
  }
}
