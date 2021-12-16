
package com.wifi.recommend

import java.text.SimpleDateFormat

import com.td.ml.xdata.common.{UserHistory, Utils, VideoProfile}
import com.wifi.recommend.utils.DataUtils.{getPreviousdate, transformTime}
import org.apache.spark.sql.SparkSession

/** .
 * Created by:jiangcx
 * Date: 2021/8/23
 * 为生成词向量，做的数据预处理，针对达人
 */
object ItemCF_talent_v2 {

  def main(args: Array[String]): Unit = {

    val argMap = Utils.parseArgs(args)
    val fromDate = argMap("from_date")
    val toDate = argMap("to_date")
    val fromDateTs = argMap("from_date_ts").toLong
    val toDateTs = argMap("to_date_ts").toLong
    val prefix = argMap("prefix")
    val min_seq_length = argMap("min_seq_length").toInt
    val seq_distinct_items = argMap("seq_distinct_items").toInt
    val tsBound = argMap("ts_bound").toLong
    val vertexFreqThres = argMap("vertex_freq_thres").toInt

    val ss = Utils.createSparkSession
    // 视频画像数据
    val video_profile = ss.sparkContext.textFile(argMap("input_path_video_profile")).map { // cstatus == 0 and ctype in (103,105,107) and createtime <= 1 month and firstcat=264
            line =>
              val profile = new VideoProfile()
              profile.parse(line)
              val vid = profile.vid
              val tmp_time = profile.createTime
              var createtime = Long.MinValue
              val firstcat = profile.firstCategory.id
              val secondcat = profile.secondCategory.id
              val talentstar = profile.userLevel
              val uid = profile.uid
              val teacher_name = profile.teacher.name
              try {
                createtime = transformTime(tmp_time)
              }catch {
                case _: Throwable => ("except_id", Map("NONE" -> 0.0))
              }
              if (talentstar>=4) {
                (vid, (uid, firstcat, teacher_name, secondcat))
              } else {
                null
              }
          }.filter(item => item != null).cache()
    val teacher_uid = video_profile.map(item => (item._2._1,1)).distinct().collectAsMap()

    val userpath = s"hdfs://10.42.31.63:8020/olap/db/user/dt=$toDate"
    val username = ss.sparkContext.textFile(userpath).map {
      line =>
        val cols = line.split('\u0001')
        (cols(0),cols(1))
    }.filter(item => teacher_uid.contains(item._1)).collectAsMap() // (uid, uname)

    // talent_star_pool
    val talent_star_pool = video_profile.filter(item=>username.contains(item._2._1)).map(item=>(item._1, item._2._1+"||"+username(item._2._1).replaceAll("\\s", ""))).collectAsMap() //(vid, (uid, firstcat, teacher_name, secondcat)
    println("valid video Pool")
    println(talent_star_pool.size)
//    val userWatch = spark.sparkContext.textFile(input_user_history).flatMap { line =>
//      val Array(diu, event) = line.split("\t")
//      val events = new UserHistory()
//      events.load(event)
//      events.actions.filter { e => e.ts > fromdt && e.played } //选择vid的条件，按照时间排序。。。
//        .map(_.vid)
//        .filter(talent_star_pool.contains(_))
//        .map(vid =>(diu, talent_star_pool(vid))).map( item => (item._1, item._2._1 + "+" + item._2._4.toString)) //(diu , tuid) -> (diu , tuid+secondcat)
//    }.map{ case(diu,tuid) => ((diu,tuid),1.0)}.reduceByKey(_+_)
//      .map(row => (row._1._1, row._1._2, row._2)).cache()
//    println("total user watch video data:")
//    println(userWatch.count())

    val rddRawSeqs = ss.sparkContext.textFile(argMap("input_path_user_history")).map {
      line =>
        val cols = line.split('\t')
        val userHistory = new UserHistory().load(cols(1))

        userHistory.actions.filter {
          event =>
            event.ts > tsBound &&
              event.playTime.getOrElse[Int](0) >= 5
        }.sortBy(_.ts).map(_.vid).toArray
    }.cache()


    // 生成字典
    val rddVocab = rddRawSeqs.flatMap(_.iterator)
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= vertexFreqThres)
      .sortBy(_._2, ascending = false, numPartitions = 1)
      .map(x => x._1)
      .cache()
    rddVocab.saveAsTextFile(argMap("output_path_vocab_dict"))
    val vocabDict = ss.sparkContext.broadcast(rddVocab.collect().toSet)

    val rddRawWalks = rddRawSeqs.map {
      x => x.filter(vocabDict.value.contains(_)).filter(talent_star_pool.contains(_)).map(talent_star_pool(_))
    }.filter {
      x =>
        x.length >= min_seq_length && x.distinct.length >= seq_distinct_items
    }.map(_.mkString(" "))
    rddRawWalks.saveAsTextFile(argMap("output_path_train_dataset"))
    }
}