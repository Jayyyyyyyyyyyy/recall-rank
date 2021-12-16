
package com.wifi.recommend

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.td.ml.xdata.common.UserHistory
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/** .
 * Created by:jiangcx
 * Date: 2021/8/23
 */
object ItemCF_user2 {
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

  def loadRealFollows(input: String): Array[String] = {
    val output: mutable.MutableList[String] = mutable.MutableList()
    val arrayRaw = JSON.parseArray(input)
    for (i <- 0 until arrayRaw.size()) {
      val item = arrayRaw.getJSONArray(i)
      output += item.getString(0)
    }
    output.toArray
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      Console.err.println("input, output, prefix, fromDate, toDate")
    }

    val Array(input, output, prefix, fromDate, toDate) = args

    val spark = SparkSession
      .builder()
      .appName("itemcf_offline")
      .enableHiveSupport()
      .getOrCreate()

//    val active_user = spark.sql(s"select id from dw.user" +
//      s" where to_date(signdate) >='$fromDate' and dt='$toDate' ")
//      .rdd.map(r => (r.getAs[Long]("id").toString, 1)).collectAsMap()
//

    val fromDateMilli = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli
    println(fromDateMilli)
    val path = s"hdfs://10.42.31.63:8020/olap/db/user/dt=$toDate"
    val active_user = spark.sparkContext.textFile(path).map {
      line =>
        val cols = line.split('\u0001')
        var signdate = Long.MinValue
        try {
          signdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(cols(15)).toInstant.toEpochMilli
        } catch {
          case _: Throwable => 0
        }
        if (signdate>=fromDateMilli ) {
          cols(0)
        } else {
          null
        }
    }.filter(uid => uid != null).map(uid => (uid, 1)).collectAsMap()

    val rdd = spark.sparkContext.textFile(input).map{ line => (line.split("\t")(0) ,loadRealFollows(line.split("\t")(1)) )
    }.filter(item => active_user.contains(item._1)).filter(item => item._2.size >= 2 && item._2.size <= 500).flatMapValues(x => x).map{ case(a,b) => ((a,b),1.0)}
    println("rdd is")
    println(rdd.count())
    val final_rdd = rdd.reduceByKey((a,b) => math.max(a, b))
      .map(row => (row._1._1, row._1._2, row._2))// diu, diu, score

//    val final_rdd = rdd.reduceByKey((a,b) => math.max(a, b))
//      .map(row => (row._1._1, row._1._2, row._2))// diu, vid, score
//      .map(row => (row._1, row._2, duration_score(row._3)))//.cache()
    // final_rdd.take(100).foreach(x => System.out.println("%s\t%s\t%f".format(x._1, x._2, x._3)))
//     println("final rdd is ")
//     println(final_rdd.count())
    val cf = new CollaborativeFiltering_user2(final_rdd, Similarity.JACCARD)
    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(11, 22)))
    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(x._1, x._2.map(r => r._1 + ":" + "%5f".format(r._2)))))
    cf.simList().map(r => prefix + r._1 + "\t" + r._2.map(r => r._1 + ":" + "%5f".format(r._2)).take(100).reduce(_ + " " + _))
      .saveAsTextFile(output + "/experiment")
    //cf.userRecommend().map(r=>r._1+"\t"+r._2.map(_._1).reduce(_+","+_)).saveAsTextFile(output+"/user")
  }
}
