package com.wifi.recommend

import com.wifi.recommend.Similarity.Similarity
import org.apache.spark.rdd.RDD


/**
 * Author: liujikun@xiaomi.com
 * Date: 2017-03-17.
 *
 * @param input RDD[(user, item)]
 * @param minUserHistory 忽略items太少的用户，默认2
 * @param maxUserHistory 截断用户太长的items，默认1000
 * @param itemSimNum item to item top num，默认100
 * @param userRecNum user to item top num，默认100
 * @param minSim 最小相似度，默认0
 * @param minCooccur 最小共现次数，默认2
 */


class CollaborativeFiltering_talent_dict(
                                  input: RDD[(String, String, Double)],
                                  itemSimNum: Int,
                                  userRecNum: Int,
                                  minSim: Int,
                                  minUserHistory: Int,
                                  maxUserHistory: Int,
                                  minItemHistory: Int,
                                  minCooccur: Int,
                                  partitions: Int,
                                  similarity: Similarity
                                )
  extends Serializable {
  //  def this(input: RDD[(String, String)]) =
  //    this(input.map(x => (x._1, x._2, Random.nextInt(5).toDouble)), 100, 100, 0, 2, 1000, 10, 2, 5000, Similarity.JACCARD)

  def this(input: RDD[(String, String, Double)]) =
    this(input, 100, 100, 0, 20, 800, 100, 100, 500, Similarity.JACCARD)

  def this(input: RDD[(String, String, Double)], similarity: Similarity) =
    this(input, 100, 100, 0, 2, 800, 100, 20, 500, similarity)
  private var simList_ : RDD[(String, Array[(String, Double)])] = _
  private val userHistory = input.coalesce(partitions).map { //coalesce只能减少分区
    //在Spark的Rdd中，Rdd是分区的。有时候需要重新设置Rdd的分区数量，比如Rdd的分区中，Rdd分区比较多，但是每个Rdd的数据量比较小，
    // 需要设置一个比较合理的分区。或者需要把Rdd的分区数量调大。还有就是通过设置一个Rdd的分区来达到设置生成的文件的数量
    case (user, item, rating) =>
      if (similarity == Similarity.JACCARD) {
        (user, (item, 1.0))
      } else {
        (user, (item, rating))
      }
  }.groupByKey()
    .map { case (user, items) => (user, items.take(maxUserHistory).toMap) } //(diu, list(tuid, freq)))
    .filter(_._2.size >= minUserHistory)
    .flatMap {
      case (user, items) =>
        for (i <- items; j <- items) yield {
          ((i._1, j._1), (i._2 * j._2, i._2, j._2, 1))
        }
    }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)).filter(_._2._4 >= minCooccur)
      .map(line => (line._1, line._2._1)).map(item=>item._1._1+"||"+item._1._2+"||"+item._2.toInt.toString).saveAsTextFile("/user/jiangcx/talent_sim/") //分子的计算
}