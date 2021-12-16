package com.wifi.recommend

import java.text.SimpleDateFormat

import com.td.ml.xdata.common.UserHistory
import org.apache.spark.sql.SparkSession

object PlayDuration {
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

    val recommendPool = spark.sql(s"select vid from da.video_cstage" +
      s" where dt>='$fromDate' and dt<='$toDate' and cstage in (6,7,8,10)")
    recommendPool.createOrReplaceTempView("vidPool")
    val vidDurationPool = spark.sql(s"select a.vid, b.duration from vidPool as a inner join dw.video as b " +
      s"on a.vid == b.vid where b.duration between 15 and 1200")
      .rdd
      .map(row => (row.getAs[Long]("vid").toString, row.getAs[Int]("duration"))).collectAsMap()
    println(vidDurationPool.size)

    val fromDateMilli = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli

    val rdd = spark.sparkContext.textFile(input).map { line =>
      val Array(diu, event) = line.split("\t")

      val events = new UserHistory()
      events.load(event)
      val tmp_events = events.actions.filter { e =>
        e.played && e.ts > fromDateMilli;
      }
//      events
      val result = tmp_events.map(item => (item.vid, item.ts, item.module))
        .filter(item => item._3 == "M011")
        .sortBy(item => item._2)
        .map(item => item._1).toList
//        .map(item => item._1)
      (diu, result)
//        .map(item => (diu, item))
//        .map(item =>((diu, item._1), updatePlayDuration(item._2, item._3)))
    }


    rdd.filter(item => item._2.nonEmpty)
      .map(line => "{\"" + line._1 + "\":" + line._2.take(5).toArray.mkString("[\"", "\", \"", "\"]") + "}")
      .saveAsTextFile(output + "/evaluation")
  }

}
