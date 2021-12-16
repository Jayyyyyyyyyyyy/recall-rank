//package com.wifi.recommend
//
//import breeze.linalg.DenseVector
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession
//import breeze.linalg.functions.cosineDistance
//
//import scala.util.hashing.MurmurHash3
//
///**
//  * Created by:liujikun
//  * Date: 2017/9/18
//  */
//object CosineSample {
//  def cosine(a: Array[Double], b: Array[Double]): Double = {
//    var dot, moda, modb = 0.0
//    for (i <- a.indices) {
//      dot += a(i) * b(i)
//      moda += a(i) * a(i)
//      modb += b(i) * b(i)
//    }
//
//    dot / math.sqrt(moda * modb)
//  }
//
//  def main(args: Array[String]): Unit = {
//    val sc = new SparkContext(new SparkConf())
//    val umap = sc.textFile("/liujikun/user_clicks/hash/").map(x => (x.split("\t")(0), x.split("\t")(1))).distinct().collectAsMap()
//    val news = sc.textFile("/liujikun/item_factors_file.bpr2").map(x => (x.split("\t")(0), DenseVector(x.split("\t")(1).split(" ").map(_.toDouble)))).cache()
//    val users = sc.textFile("/liujikun/user_factors_file.bpr2").map(x => (umap(x.split("\t")(0)), DenseVector(x.split("\t")(1).split(" ").map(_.toDouble))))
//
//    val samp=users.take(20).flatMap { case (u, v) =>
//      news.mapPartitions{ls =>
////        val doc = new StaticNewsClient()
////        val config = ConfigFactory.load(classOf[NewsProfileClient].getClassLoader).getConfig("news-store.static-server")
////        doc.Init(config)
////        val ret=ls.map(l=>(l._1, cosineDistance(l._2, v), doc.getDocument(l._1).getTitle, doc.getDocument(l._1).getUrlLocal))
////        doc.close()
//        ret
//      }.filter(_._2 > 0.0).sortBy(_._2).take(100) :+ ("------------- " + u + " -------------", 0.0, "","")
//    }
//    sc.parallelize(samp, 100).saveAsTextFile("/liujikun/bpr.sample.cos")
//
//  }
//
//}
