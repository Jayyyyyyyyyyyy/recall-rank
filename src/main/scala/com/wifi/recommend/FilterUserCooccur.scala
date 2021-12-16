package com.wifi.recommend

import org.apache.spark.{SparkConf, SparkContext}


object FilterUserCooccur {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      Console.err.println("likes, output")
    }
    val Array(likes, output) = args
    val conf = new SparkConf().setAppName("UserLikes")
    conf.set("spark.hadoop.io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec")
    val sc = new SparkContext(conf)

    val userLikes=sc.textFile(likes)
    val vocabrdd = userLikes.flatMap(_.split("\t")(1).split(" ").map(_.split(":")(0)))
      .distinct().zipWithIndex().mapValues(_.toInt).cache()
    val vocab = vocabrdd.collectAsMap()
    val bvocab = sc.broadcast(vocab)
    vocabrdd.saveAsTextFile(output + "/vocab")

    userLikes.flatMap { likes =>
      val likeSeq = likes.split("\t")(1).split(" ").map(_.split(":")).take(10).map(i => (bvocab.value(i(0)), i(1).toDouble)).sortWith(_._1 < _._1)
      for (i <- likeSeq.indices; j <- likeSeq.indices.drop(i + 1)) yield {
        ((likeSeq(i)._1, likeSeq(j)._1), math.min(likeSeq(i)._2.asInstanceOf[Double], likeSeq(j)._2.asInstanceOf[Double]))
      }
    }.flatMap {
      case ((i, j), sim) =>
        Seq(((i, j), sim), ((j, i), sim))
    }.reduceByKey(_ + _).map { case ((i, j), c) =>
      i + " " + j + " " + math.log1p(c)
    }.saveAsTextFile(output + "/cooccur")
  }
}
