package com.wifi.recommend

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating


/**
  * Created by:liujikun
  * Date: 2017/9/18
  */
object WifiALS {

  def cosine(a: Array[Double], b: Array[Double]): Double = {
    var dot, moda, modb = 0.0
    for (i <- a.indices) {
      dot += a(i) * b(i)
      moda += a(i)*a(i)
      modb += b(i)*b(i)
    }

    dot / math.sqrt(moda * modb)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      Console.err.println("input, output")
    }

    //val Array(intput,output)=args
    val conf = new SparkConf().setAppName("ALS")
    val sc = new SparkContext(conf)

    val input = "/liujikun/tags.cooccur"
    val output = "/liujikun/tags"

    // Load and parse the data
    val data = sc.textFile(input)

    val ratings = data.map(_.split(' ') match { case Array(i, j, rate) =>
      Rating(i.toInt, j.toInt, rate.toDouble)
    })
    val rank = 100
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    model.userFeatures.map(r => r._1 + "\t" + r._2.map(_.toString).reduce(_ + "," + _)).saveAsTextFile(output + ".vectors")
    model.userFeatures.cartesian(model.userFeatures).filter {
      case ((i, iv), (j, jv)) =>
        i != j
    }.map {
      case ((i, iv), (j, jv)) =>
        (i, (j, cosine(iv, jv)))
    }.groupByKey().map(x => x._1 + "\t" + x._2.toArray.sortWith(_._2 > _._2).take(10).map(f => f._1 + ":" + f._2).reduce(_ + " " + _)).coalesce(10).saveAsTextFile(output + ".sim")
    model.recommendProductsForUsers(5).map(r => r._1 + "\t" + r._2.map(x => x.user + ":" + x.product + ":" + x.rating).reduce(_ + " " + _)).coalesce(10).saveAsTextFile(output + ".sim")
  }


}
