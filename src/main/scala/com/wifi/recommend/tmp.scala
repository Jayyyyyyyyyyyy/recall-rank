
package com.wifi.recommend

import com.td.ml.xdata.common.{UserHistory, Utils, VideoProfile}
import com.wifi.recommend.utils.DataUtils.transformTime

/** .
 * Created by:jiangcx
 * Date: 2021/8/23
 * 为生成词向量，做的数据预处理，针对达人
 */
object tmp {

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
    val ctype_used =  List(105, 106, 107, 501, 502, 503)
    val video_profile = ss.sparkContext.textFile(argMap("input_path_video_profile")).map { // cstatus == 0 and ctype in (103,105,107) and createtime <= 1 month and firstcat=264
            line =>
              val profile = new VideoProfile()
              profile.parse(line)
              val vid = profile.vid
              val tmp_time = profile.createTime
              val cstatus = profile.cstatus
              var createtime = Long.MinValue
              val firstcat = profile.firstCategory.id
              val secondcat = profile.secondCategory.id
              val content_mp3 = profile.mp3.name
              val talentstar = profile.userLevel
              val ctype = profile.ctype
              val uid = profile.uid
              val teacher_name = profile.teacher.name
              val video_mp3_name = profile.mp3
              try {
                createtime = transformTime(tmp_time)
              }catch {
                case _: Throwable => ("except_id", Map("NONE" -> 0.0))
              }
              if (talentstar >= 4 && firstcat==264 && content_mp3!="" && cstatus==0 && !ctype_used.contains(ctype) && createtime> 1622476800000L) {

                vid
              } else {
                null
              }
          }.filter(item => item != null).cache()

    video_profile.saveAsTextFile("/user/jiangcx/tmp_delete")
    }
}