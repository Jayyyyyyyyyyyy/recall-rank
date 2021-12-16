package com.wifi.recommend

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import java.util.{Calendar, Date}

import com.td.ml.xdata.common.UserHistory
import com.wifi.recommend.ItemCF_offline.getBetweenDates
import org.apache.spark.sql.SparkSession
import org.dmg.pmml.False

/** .
 * Created by:liujikun
 * Date: 2017/9/18
 */
object ItemCF_experiment {
  def duration_score(playDuration: Int): Double = {
     math.atan(playDuration/180.0)
//    1/(1 + math.exp(-playDuration/180.0))
    // 1/(1 + math.exp(playDuration/55.0-5))
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


  def updatePlayDuration(playDuration: Int, isImmersiveVideo: Boolean): Int ={
    if (isImmersiveVideo) {
      val dif = playDuration - 6
      if (dif > 0){
        return dif
      }
      else {
        return 1
      }
    }
    playDuration
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      Console.err.println("input, output, prefix, fromDate, toDate")
    }

    val Array(input, output, prefix, fromDate, toDate) = args

    val spark = SparkSession
      .builder()
      .appName("itemcf")
      .enableHiveSupport()
      .getOrCreate()
//    val recommendPool = spark.sql(s"select vid from da.video_cstage" +
//      s" where dt>='$fromDate' and dt<='$toDate' and cstage in (6,7,8,10)")
//      .rdd.map(r => (r.getAs[Long]("vid").toString, 1)).collectAsMap()
//    val fromDateMilli = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli

    val fromdt = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli
    val cstage_used =  List(6,7,8,10)
    val all_date = getBetweenDates(fromDate,toDate)
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

    val rdd = spark.sparkContext.textFile(input).flatMap { line =>
      val Array(diu, event) = line.split("\t")
      val events = new UserHistory()
      events.load(event)
      events.actions.filter { e =>
        e.ts > fromdt && e.played
      }.map(item => (item.vid, item.playTime.getOrElse[Int](0), false))
        .filter(item => recommendPool.contains(item._1))
        .map(item =>((diu, item._1), updatePlayDuration(item._2, item._3)))
    }

    //    val rdd = spark.sql(s"select diu,vid,expose from da.user_profile_view_behavior " +
    //      s"where dt>='$fromDate' and dt<='$toDate' and play is not null and diu is not null and vid is not null")
    //      .filter { r =>
    //        r.getAs[String]("expose")
    //          .matches(".*(发现广场舞\",\"推荐|发现广场舞\",\"为你推荐-大屏|播放页\",\"相关推荐).*")
    //      }.rdd.map(r => (r.getAs[String]("diu"), r.getAs[Long]("vid").toString))
    //      .filter(r => recommendPool.contains(r._2))
    //      .distinct()
    val final_rdd = rdd.reduceByKey((a,b) => math.max(a, b))
      .map(row => (row._1._1, row._1._2, row._2))
      .map(row => (row._1, row._2, duration_score(row._3)))
    //    val rdd = spark.sql(s"select diu,vid,expose from da.user_profile_view_behavior " +
    //      s"where dt>='$fromDate' and dt<='$toDate' and play is not null and diu is not null and vid is not null")
    //      .filter { r =>
    //        r.getAs[String]("expose")
    //          .matches(".*(发现广场舞\",\"推荐|发现广场舞\",\"为你推荐-大屏|播放页\",\"相关推荐).*")
    //      }.rdd.map(r => (r.getAs[String]("diu"), r.getAs[Long]("vid").toString))
    //      .filter(r => recommendPool.contains(r._2))
    //      .distinct()

    //    val Array(trainingData, testData) = final_rdd.randomSplit(Array(0.8, 0.2), seed = 1234L)
    //    // diu => [(vid, score), (vid, score)], ...
    //    if (!pathIsExist(spark, output + "/trainset")) {
    //      trainingData.map { case (diu, vid, score) => (diu, (vid, score)) }.groupByKey().saveAsTextFile(output + "/trainset")
    //    }
    //    if (!pathIsExist(spark, output + "/testset")) {
    //      testData.map { case (diu, vid, score) => (diu, (vid, score)) }.groupByKey().saveAsTextFile(output + "/testset")
    //    }

    println("final rdd is ")
    println(final_rdd.count())

    val cf = new CollaborativeFiltering_jcx(final_rdd)


    cf.simList().map(r => prefix + r._1 + "\t" + r._2.map(r => r._1 + ":" + "%5f".format(r._2)).take(100).reduce(_ + " " + _))
      .saveAsTextFile(output + "/experiment")
    //cf.userRecommend().map(r=>r._1+"\t"+r._2.map(_._1).reduce(_+","+_)).saveAsTextFile(output+"/user")
  }

}
