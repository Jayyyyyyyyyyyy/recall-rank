
package com.wifi.recommend

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import java.util.{Calendar, Date}
import com.td.ml.xdata.common.UserHistory
import com.wifi.recommend.ItemCF_experiment.updatePlayDuration
import org.apache.spark.sql.SparkSession

/** .
 * Created by:liujikun
 * Date: 2017/9/18
 */
object ItemCF_offline {
  def duration_score(playDuration: Double): Double = {
    // math.atan(playDuration/360.0)
    //    1/(1 + math.exp(-playDuration/180.0))
    playDuration/1.0
  }
  def pathIsExist(spark: SparkSession, path: String): Boolean = {
    //取文件系统
    val filePath = new org.apache.hadoop.fs.Path( path )
    val fileSystem = filePath.getFileSystem( spark.sparkContext.hadoopConfiguration )

    // 判断路径是否存在
    fileSystem.exists( filePath )
  }

  def getBetweenDates(start: String, end: String) = {
    val startData = new SimpleDateFormat("yyyy-MM-dd").parse(start); //定义起始日期
    val endData = new SimpleDateFormat("yyyy-MM-dd").parse(end); //定义结束日期

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var buffer = new ListBuffer[String]
    buffer += dateFormat.format(startData.getTime())
    val tempStart = Calendar.getInstance()

    tempStart.setTime(startData)
    tempStart.add(Calendar.DAY_OF_YEAR, 1)

    val tempEnd = Calendar.getInstance()
    tempEnd.setTime(endData)
    while (tempStart.before(tempEnd)) {
      // result.add(dateFormat.format(tempStart.getTime()))
      buffer += dateFormat.format(tempStart.getTime())
      tempStart.add(Calendar.DAY_OF_YEAR, 1)
    }
    buffer += dateFormat.format(endData.getTime())
    buffer.toList
  }

  def score_strategy(isDownload: Int, isFav: Int ): Double ={

      if (isDownload > 0 || isFav > 0) {
        return 1.001
      }
      else{
        return 0.999
      }
    //    if (playDuration >= 360) {
    //      return 1 + Download + Fav
    //    }
    //    else{
    //      return 0 + Download + Fav
    //    }
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      Console.err.println("input, output, prefix, fromDate, toDate, oneweek")
    }

    val Array(input, output, prefix, fromDate, toDate, oneweek) = args

    val spark = SparkSession
      .builder()
      .appName("itemcf_offline")
      .enableHiveSupport()
      .getOrCreate()

//
//    val recommendPool = spark.sql(s"select vid from da.video_cstage" +
//      s" where dt>='$fromDate' and dt<='$toDate' and cstage in (6,7,8,10) ")
//      .rdd.map(r => (r.getAs[Long]("vid").toString, 1)).collectAsMap()
//

    val fromdt = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli
    val cstage_used =  List(6,7,8,10)
    val all_date = getBetweenDates(oneweek,toDate)
    val path = all_date.map( x => s"hdfs://10.42.31.63:8020/user/zhangjl/olap/da/video_cstage/dt=$x").mkString(",")
    println(path)

    val recommendPool = spark.sparkContext.textFile(path).map {
      line =>
        val cols = line.split('\u0001')
        val utime = cols(0).toLong
        val vid = cols(1)
        val cstage = cols(2).toInt
        if (cstage_used.contains(cstage) ) {
          vid
        } else {
          null
        }
    }.filter(item => item != null).map(vid => (vid, 1)).collectAsMap()
    println(recommendPool.size)

//    val fromDateMilli = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli
//    println(fromDateMilli)
    val rdd = spark.sparkContext.textFile(input).flatMap { line =>
      val Array(diu, event) = line.split("\t")
      val events = new UserHistory()
      events.load(event)
      events.actions.filter { e =>e.ts > fromdt && e.played}
        .map(item => (item.vid, item.isDownload, item.isFav))
        .filter(item => recommendPool.contains(item._1))
        .map(item =>((diu, item._1), score_strategy(item._2, item._3)))
    }
    println("rdd is")
    println(rdd.count())

    //    val rdd = spark.sql(s"select diu,vid,expose from da.user_profile_view_behavior " +
    //      s"where dt>='$fromDate' and dt<='$toDate' and play is not null and diu is not null and vid is not null")
    //      .filter { r =>
    //        r.getAs[String]("expose")
    //          .matches(".*(发现广场舞\",\"推荐|发现广场舞\",\"为你推荐-大屏|播放页\",\"相关推荐).*")
    //      }.rdd.map(r => (r.getAs[String]("diu"), r.getAs[Long]("vid").toString))
    //      .filter(r => recommendPool.contains(r._2))
    //      .distinct()
    val final_rdd = rdd.reduceByKey((a,b) => math.max(a, b))
      .map(row => (row._1._1, row._1._2, row._2))// diu, vid, score
      .map(row => (row._1, row._2, duration_score(row._3)))//.cache()
    // final_rdd.take(100).foreach(x => System.out.println("%s\t%s\t%f".format(x._1, x._2, x._3)))
    // println("final rdd is ")
    // println(final_rdd.count())
    val cf = new CollaborativeFiltering_off(final_rdd)
    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(11, 22)))
    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(x._1, x._2.map(r => r._1 + ":" + "%5f".format(r._2)))))
    println("finish")
    cf.simList().map(r => prefix + r._1 + "\t" + r._2.map(r => r._1 + ":" + "%5f".format(r._2)).take(100).reduce(_ + " " + _))
      .saveAsTextFile(output + "/experiment")
    //cf.userRecommend().map(r=>r._1+"\t"+r._2.map(_._1).reduce(_+","+_)).saveAsTextFile(output+"/user")
  }
}
