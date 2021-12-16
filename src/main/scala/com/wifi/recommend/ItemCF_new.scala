//package com.wifi.recommend
//
//import java.text.SimpleDateFormat
//import com.td.ml.xdata.common.UserHistory
//import org.apache.spark.sql.SparkSession
//
///** .
// * Created by:liujikun
// * Date: 2017/9/18
// */
//object ItemCF_new {
//  def main(args: Array[String]): Unit = {
//    if (args.length < 2) {
//      Console.err.println("input, output, prefix, fromDate, toDate")
//    }
//
//    val Array(input, output, prefix, fromDate, toDate) = args
//
//    val spark = SparkSession
//      .builder()
//      .appName("itemcf")
//      .enableHiveSupport()
//      .getOrCreate()
//    val recommendPool = spark.sql(s"select vid from da.video_cstage" +
//      s" where dt>='$fromDate' and dt<='$toDate' and cstage in (6,7,8,10)")
//      .rdd.map(r => (r.getAs[Long]("vid").toString, 1)).collectAsMap()
//
//    val fromDateMilli = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli
//
//    val rdd = spark.sparkContext.textFile(input).flatMap { line =>
//      val Array(diu, event) = line.split("\t")
//      val events = new UserHistory().load(event).eventQueue.filter { e =>
//        e.play && e.playTs.getOrElse(0L) > fromDateMilli;
//      }
//      events.map(_.vid).filter(recommendPool.contains).distinct.map(vid => (diu, vid))
//    }
//    //
//    //    val rdd = spark.sql(s"select diu,vid,expose from da.user_profile_view_behavior " +
//    //      s"where dt>='$fromDate' and dt<='$toDate' and play is not null and diu is not null and vid is not null")
//    //      .filter { r =>
//    //        r.getAs[String]("expose")
//    //          .matches(".*(发现广场舞\",\"推荐|发现广场舞\",\"为你推荐-大屏|播放页\",\"相关推荐).*")
//    //      }.rdd.map(r => (r.getAs[String]("diu"), r.getAs[Long]("vid").toString))
//    //      .filter(r => recommendPool.contains(r._2))
//    //      .distinct()
//
//    val cf = new CollaborativeFiltering(rdd)
//    cf.simList().map(r => prefix + r._1 + "\t" + r._2.map(r => r._1 + ":" + "%5f".format(r._2)).take(100).reduce(_ + " " + _))
//      .saveAsTextFile(output + "/item")
//    //cf.userRecommend().map(r=>r._1+"\t"+r._2.map(_._1).reduce(_+","+_)).saveAsTextFile(output+"/user")
//  }
//
//}
