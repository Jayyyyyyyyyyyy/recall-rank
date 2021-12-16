
package com.wifi.recommend
import java.text.SimpleDateFormat
import com.td.ml.xdata.common.{UserHistory, VideoProfile}
import com.wifi.recommend.utils.DataUtils.{transformTime,getPreviousdate}
import org.apache.spark.sql.SparkSession
/** .
 * Created by:jiangcx
 * Date: 2021/8/23
 */
object ItemCF_trend {

  def pathIsExist(spark: SparkSession, path: String): Boolean = {
    //取文件系统
    val filePath = new org.apache.hadoop.fs.Path( path )
    val fileSystem = filePath.getFileSystem( spark.sparkContext.hadoopConfiguration )
    // 判断路径是否存在
    fileSystem.exists( filePath )
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      Console.err.println("input, output, prefix, fromDate, toDate")
    }

    val Array(input, output, prefix, fromDate, toDate) = args

    val spark = SparkSession
      .builder()
      .appName("itemcf_trend")
      .enableHiveSupport()
      .getOrCreate()

    val fromdt = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli
    val one_month = getPreviousdate(toDate,90, patten = "yyyy-MM-dd")
    println(one_month)
    val one_month_ts = new SimpleDateFormat("yyyy-MM-dd").parse(one_month).toInstant.toEpochMilli
    println(one_month_ts)

    val ctype_used =  List(501,502,503,103,105,107,109,111,112)
    val path = s"/user/jiaxj/Apps/DocsInfo/$toDate/*"
    val recommendPool = spark.sparkContext.textFile(path).map {
            line =>
              val profile = new VideoProfile()
              profile.parse(line)
              val vid = profile.vid
              val uid = profile.uid
              val ctype = profile.ctype
              val cstatus = profile.cstatus
              val tmp_time = profile.createTime
              var createtime = Long.MinValue
              val duration = profile.length
              try {
                createtime = transformTime(tmp_time)
              }catch {
                case _: Throwable => ("except_id", Map("NONE" -> 0.0))
              }
              if ((cstatus==0) && ctype_used.contains(ctype) && createtime>one_month_ts) {
//              if ((cstatus==0) && ctype_used.contains(ctype)) {
                (vid, duration)
              } else {
                null
              }
          }.filter(item => item != null).collectAsMap()
    // vid uid dict
    val vid_uid_dict = spark.sparkContext.textFile(path).map {
      line =>
        val profile = new VideoProfile()
        profile.parse(line)
        val vid = profile.vid
        val uid = profile.uid.toString
        (vid, uid)
    }.collectAsMap()

    println("recommendPool")
    println(recommendPool.size)
    val rdd = spark.sparkContext.textFile(input).flatMap { line =>
      val Array(diu, event) = line.split("\t")
      val events = new UserHistory()
      events.load(event)
      events.actions.filter { e => e.ts > fromdt && e.played } //选择vid的条件，按照时间排序。。。
        .filter(item => recommendPool.contains(item.vid))
        .filter(item => item.playTime.getOrElse[Int](0)*1.0/recommendPool(item.vid)>=0.1)
        .map(item =>((diu, item.vid),1.0))
    }
    println("rdd is")
    println(rdd.count())

    val final_rdd = rdd.reduceByKey((a,b) => math.max(a, b))
      .map(row => (row._1._1, row._1._2, row._2))// diu, vid, score
    // final_rdd.take(100).foreach(x => System.out.println("%s\t%s\t%f".format(x._1, x._2, x._3)))
    // println("final rdd is ")
    // println(final_rdd.count())
    val cf = new CollaborativeFiltering_trend(final_rdd)
    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(11, 22)))
    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(x._1, x._2.map(r => r._1 + ":" + "%5f".format(r._2)))))
    println("finish")
    cf.simList().map(r => prefix + r._1 + "\t" + r._2.map(r => r._1 + ":" + "%5f".format(r._2) +":"+ "%s".format(vid_uid_dict(r._1))).take(200).reduce(_ + " " + _))
      .saveAsTextFile(output + "/trend")
    //cf.userRecommend().map(r=>r._1+"\t"+r._2.map(_._1).reduce(_+","+_)).saveAsTextFile(output+"/user")
  }
}