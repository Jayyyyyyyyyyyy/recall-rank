package com.wifi.recommend

import com.wifi.recommend.Similarity.Similarity
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.math.sqrt
import scala.util.Random


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


class CollaborativeFiltering_off(
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
    this(input, 100, 100, 0, 2, 1000, 10, 2, 5000, Similarity.COSINE)

  def this(input: RDD[(String, String, Double)], similarity: Similarity) =
    this(input, 100, 100, 0, 2, 1000, 10, 2, 5000, similarity)
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
    .map { case (user, items) => (user, items.take(maxUserHistory).toMap) }
    .filter(_._2.size >= minUserHistory)
    .cache()

//  userHistory.saveAsTextFile("/user/jiangcx/vector/video_itemcf4")

  /**
   *
   * @return RDD[(item, (item, sim))]
   */
  private def simPair(): RDD[(String, (String, Double))] = {
    val userIUF = userHistory.map(line => (line._1, line._2.size)).collectAsMap()
    val itemsCount = userHistory.flatMap {
      case (user, items) => items.map(item => (item._1, (item._2 * item._2, 1)))}
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).filter(_._2._2 >= minItemHistory)
      .map(line => (line._1, line._2._1)).collectAsMap()
    println("size of output:")
    println(itemsCount.size)
    val matrix = userHistory.flatMap {
      case (user, items) =>
        for (i <- items.filter(_._2<1); j <- items.filter(_._2>1)) yield {
          ((i._1, j._1), (i._2 * j._2, 1))
        }
    }
//    matrix.saveAsTextFile("/user/jiangcx/vector/video_itemcf3")

    val mtx = matrix.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).filter(_._2._2 >= minCooccur)
      .map(line => (line._1, line._2._1))

    mtx.map {
      case ((i, j), dot) =>
        val sim = {
          if (itemsCount.contains(i) && itemsCount.contains(j)) {
            if (similarity == Similarity.JACCARD) {
              dot/ (itemsCount(i) + itemsCount(j) - dot)
            } else {
              dot/ (itemsCount(i) + itemsCount(j) - dot)
            }
          }
          else 0.0
        }
        (i, (j, sim))
    }.filter(_._2._2 > minSim)

  }

  /**
   * @return RDD[(item, Array(item, sim))]
   */
  def simList(): RDD[(String, Array[(String, Double)])] = {
    if (simList_ != null)
      return simList_

    simList_ = simPair().groupByKey().map {
      case (i, iter) =>
        val simItems = iter.toArray.sortWith(_._2 > _._2).take(itemSimNum)
        (i, simItems)
    }
    simList_.cache()

  }

  /**
   * @return RDD[(user, Array(item, sim))]
   */
  def userRecommend(): RDD[(String, Array[(String, Double)])] = {
    userHistory.flatMap {
      case (user, items) => items.map { case (item, rating) => (item, (user, rating)) }
    }.join(simList()).map {
      case (item, ((user, rating), simItemList)) =>
        val fullItemList = simItemList.map { case (simItem, sim) => (rating, simItem, sim) }
        (user, fullItemList)
    }.reduceByKey(_ ++ _).join(userHistory).map {
      case (user, (fullItemList, userHistoryMap)) =>
        val itemRecsMap = mutable.HashMap[String, Double]()
        fullItemList.foreach {
          case (rating, simItem, sim) =>
            if (!userHistoryMap.contains(simItem)) {
              if (!itemRecsMap.contains(simItem)) {
                itemRecsMap(simItem) = sim * rating
              } else {
                itemRecsMap(simItem) += sim * rating
              }
            }
        }

        val itemRecsList = itemRecsMap.toArray.sortWith(_._2 > _._2).take(userRecNum)
        (user, itemRecsList)
    }
  }


}