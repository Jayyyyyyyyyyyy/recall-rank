
package com.wifi.recommend

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.td.ml.xdata.common.{UserHistory, VideoProfile}
import com.wifi.recommend.utils.DataUtils.{getPreviousdate, transformTime}
import org.apache.spark.sql.SparkSession
import org.dmg.pmml.True

import scala.collection.mutable

/** .
 * Created by:jiangcx
 * Date: 2021/8/23
 */
object ItemCF_talent {

  def pathIsExist(spark: SparkSession, path: String): Boolean = {
    //取文件系统
    val filePath = new org.apache.hadoop.fs.Path( path )
    val fileSystem = filePath.getFileSystem( spark.sparkContext.hadoopConfiguration )
    // 判断路径是否存在
    fileSystem.exists( filePath )
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      Console.err.println("input_user_follow, output, prefix, fromDate, toDate, input_user_history")
    }

    val Array(input_user_follow, output, prefix, fromDate, toDate, input_user_history) = args

    val spark = SparkSession
      .builder()
      .appName("follow_watch")
      .enableHiveSupport()
      .getOrCreate()

    val fromdt = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli  //传入
    val one_month = getPreviousdate(toDate,30, patten = "yyyy-MM-dd")// 30天前
    val one_month_ts = new SimpleDateFormat("yyyy-MM-dd").parse(one_month).toInstant.toEpochMilli


    val path = s"/user/jiaxj/Apps/DocsInfo/$toDate/*"
    val video_profile = spark.sparkContext.textFile(path).map { // cstatus == 0 and ctype in (103,105,107) and createtime <= 1 month and firstcat=264
            line =>
              val profile = new VideoProfile()
              profile.parse(line)
              val vid = profile.vid
              val ctype = profile.ctype
              val cstatus = profile.cstatus
              val tmp_time = profile.createTime
              var createtime = Long.MinValue
              val duration = profile.length
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

    val teacher_name = video_profile.map(item => (item._2._1,1)).distinct().collectAsMap()

    val userpath = s"hdfs://10.42.31.63:8020/olap/db/user/dt=$toDate"
    val username = spark.sparkContext.textFile(userpath).map {
      line =>
        val cols = line.split('\u0001')
        (cols(0),cols(1))
    }.filter(item => teacher_name.contains(item._1)).collectAsMap() // uid, uname

    val other_info_rdd = video_profile.filter(item => username.contains(item._2._1)).map(item => ((item._2._1, username(item._2._1), item._2._2), 1)).reduceByKey(_+_) // ((uid, uname, firscat),1)
      .map(item => ((item._1._1, item._1._2), (item._1._3, item._2))).groupByKey().map{ //((uid, uname), (firscat, sum))
      case (i, iter) =>
        val simItems = iter.toArray.sortWith(_._2 > _._2).head
        (i._1, simItems._1, i._2) //(uid, firstcat, uname)
    }.map(item => (item._1, "||"+item._2.toString +"||"+item._3)).distinct().collectAsMap() //(uid, ||firstcat||uanme)
    // talent_star_pool
    val talent_star_pool = video_profile.collectAsMap() //(vid, (uid, firstcat, teacher_name, secondcat)
    println("valid video Pool")
    println(talent_star_pool.size)

    val userWatch = spark.sparkContext.textFile(input_user_history).flatMap { line =>
      val Array(diu, event) = line.split("\t")
      val events = new UserHistory()
      events.load(event)
      events.actions.filter { e => e.ts > fromdt && e.played } //选择vid的条件，按照时间排序。。。
        .map(_.vid)
        .filter(talent_star_pool.contains(_))
        .map(vid =>(diu, talent_star_pool(vid))).map( item => (item._1, item._2._1 + "+" + item._2._4.toString)) //(diu , tuid) -> (diu , tuid+secondcat)
    }.map{ case(diu,tuid) => ((diu,tuid),1.0)}.reduceByKey(_+_)
      .map(row => (row._1._1, row._1._2, row._2)).cache()
    println("total user watch video data:")
    println(userWatch.count())

    //统计大人的粉丝群体
//    userWatch.map(item => (item._2, item._1)).groupByKey()
//      .map {
//        case (i, iter) =>
//          val format_line = iter.toArray.reduce(_ + " " + _)
//          prefix + i + "\t" + format_line
//      }.saveAsTextFile(output + "/talentstar_fans_diu")
//    if (1==1){
//    new CollaborativeFiltering_talent_dict(userWatch, Similarity.COSINE)
//    }
    val cf = new CollaborativeFiltering_user(userWatch, Similarity.COSINE)
    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(11, 22)))
    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(x._1, x._2.map(r => r._1 + ":" + "%5f".format(r._2)))))
    cf.simList().map(r => prefix + r._1+other_info_rdd(r._1.split("\\+")(0)) + "\t" + r._2.map(r => r._1 + "||" + "%d".format(r._2.toInt)+other_info_rdd(r._1.split("\\+")(0))).take(100).reduce(_ + " " + _))
      .saveAsTextFile(output + "/talent")
    }

}