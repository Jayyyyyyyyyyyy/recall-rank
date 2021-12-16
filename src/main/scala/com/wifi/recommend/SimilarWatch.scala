//
//package com.wifi.recommend
//
//import java.text.SimpleDateFormat
//
//import com.alibaba.fastjson.JSON
//import com.td.ml.xdata.common.{UserHistory, VideoProfile}
//import com.wifi.recommend.utils.DataUtils.{getPreviousdate, transformTime}
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable
//
///** .
// * Created by:jiangcx
// * Date: 2021/8/23
// */
//object SimilarWatch {
//
//  def pathIsExist(spark: SparkSession, path: String): Boolean = {
//    //取文件系统
//    val filePath = new org.apache.hadoop.fs.Path( path )
//    val fileSystem = filePath.getFileSystem( spark.sparkContext.hadoopConfiguration )
//    // 判断路径是否存在
//    fileSystem.exists( filePath )
//  }
//
//  def main(args: Array[String]): Unit = {
//    if (args.length < 2) {
//      Console.err.println("input_user_follow, output, prefix, fromDate, toDate, input_user_history")
//    }
//
//    val Array(input_user_follow, output, prefix, fromDate, toDate, input_user_history) = args
//
//    val spark = SparkSession
//      .builder()
//      .appName("follow_watch")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    val fromdt = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli // 30天前
//    val one_month = getPreviousdate(toDate,30, patten = "yyyy-MM-dd")
//    val one_month_ts = new SimpleDateFormat("yyyy-MM-dd").parse(one_month).toInstant.toEpochMilli
//
//    val ctype_used =  List(103,105,107)
//    val path = s"/user/jiaxj/Apps/DocsInfo/$toDate/*"
//    val validPool = spark.sparkContext.textFile(path).map { // cstatus == 0 and ctype in (103,105,107) and createtime <= 1 month and firstcat=264
//            line =>
//              val profile = new VideoProfile()
//              profile.parse(line)
//              val vid = profile.vid
//              val ctype = profile.ctype
//              val cstatus = profile.cstatus
//              val tmp_time = profile.createTime
//              var createtime = Long.MinValue
//              val duration = profile.length
//              val firstcat = profile.firstCategory.id
//              try {
//                createtime = transformTime(tmp_time)
//              }catch {
//                case _: Throwable => ("except_id", Map("NONE" -> 0.0))
//              }
//              if ((firstcat==264) && (cstatus==0) && ctype_used.contains(ctype) && createtime>one_month_ts) {
//                (vid, duration)
//              } else {
//                null
//              }
//          }.filter(item => item != null).collectAsMap()
//    println("valid video Pool")
//    println(validPool.size)
//
//    val userWatch = spark.sparkContext.textFile(input_user_history).flatMap { line =>
//      val Array(diu, event) = line.split("\t")
//      val events = new UserHistory()
//      events.load(event)
//      events.actions.filter { e => e.ts > fromdt && e.played } //选择vid的条件，按照时间排序。。。
//        .map(item => (item.vid, item.isDownload, item.isFav, item.playTime.getOrElse[Int](0)))
//        .filter(item => validPool.contains(item._1))
//        .filter(item => item._4*1.0/validPool(item._1)>=0.7 || item._2 == 1 || item._3 == 1) //播放率
//        .map(item =>(diu, item._1))
//    }.groupByKey()
////      .filter(item => item._2.size > 10)
//      .collectAsMap()
//    println("total user watch video data:")
//    println(userWatch.size)
//    userWatch.take(10).foreach(x => println("%s -> %s".format(x._1, x._2.toList.mkString(";"))))
//
//
//
//    // #################################################  user ###############################################################################################
//    def loadRealFollows(input: String): Array[String] = {
//      val output: mutable.MutableList[String] = mutable.MutableList()
//      val arrayRaw = JSON.parseArray(input)
//      for (i <- 0 until math.min(arrayRaw.size(), 100)) {
//        val item = arrayRaw.getJSONArray(i)
//        if (item.getString(6) != ""){
//          output += item.getString(6)
//        }
//      }
//      output.toArray
//    }
//
//    val userpath = s"hdfs://10.42.31.63:8020/olap/db/user/dt=$toDate"
//    val active_user = spark.sparkContext.textFile(userpath).map {
//      line =>
//        val cols = line.split('\u0001')
//        var signdate = Long.MinValue
//        try {
//          signdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(cols(15)).toInstant.toEpochMilli
//        } catch {
//          case _: Throwable => 0
//        }
//        if (signdate>=one_month_ts ) {  //一个月内激活的用户
//          cols(0)
//        } else {
//          null
//        }
//    }.filter(uid => uid != null).map(uid => (uid, 1)).collectAsMap()
//    println("active users:")
//    println(active_user.size)
//    //uid2diu字典
//    val total_user_uid_diu = spark.sparkContext.textFile(userpath).map {
//      line =>
//        val cols = line.split('\u0001')
//        (cols(0),cols(33)) // (uid, diu)
//    }.collectAsMap()
//    //diu2uid字典
//    val total_user_uid_diu2 = spark.sparkContext.textFile(userpath).map {
//      line =>
//        val cols = line.split('\u0001')
//        (cols(33),cols(0)) // (diu, uid)
//    }.collectAsMap()
//    //30万白名单用户导入
//    val test_user_uid = spark.sparkContext.textFile(s"hdfs://10.42.31.63:8020/user/jiangcx/out.txt")
//      .filter(item=>total_user_uid_diu2.contains(item.trim()))
//      .map(item=> (total_user_uid_diu2(item.trim()),1)).collectAsMap() // diu 2 uid
//    println("white users have uid:")
//    println(test_user_uid.size)  //30万diu中， 含有uid的个数
//    val rdd = spark.sparkContext.textFile(input_user_follow)
//      .map{ line => (line.split("\t")(0) ,loadRealFollows(line.split("\t")(1)) )}
////      .filter(item => item._2.size != 0) //过滤是否为激活用户
//      .filter(item => active_user.contains(item._1)) //过滤是否为激活用户
//      .filter(item => test_user_uid.contains(item._1)).cache() // 测试过滤  可以删除
//       println("after filter white user")
//       println(rdd.count())
//    val rdd2 = rdd.filter(item => item._2.size >= 20)  //关注列表个数大于20
//      .flatMapValues(x => x) //(uid, uid)
//      .map{ case(a,b) => ((a,b),1.0)}
//      .reduceByKey((a,b) => math.max(a, b))
//      .map(row => (row._1._1, row._1._2, row._2))
//
//    val cf = new CollaborativeFiltering_user(rdd2)
//    val simiar_users = cf.simList().map(item => (item._1, item._2.take(100))).flatMap{
//      case (diu, dius) => dius.map(item => (diu, userWatch(diu)))}
//
//      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).filter(_._2._2 >= minItemHistory)
//      .map(line => (line._1, line._2._1)).collectAsMap()
//
//
//
//    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(11, 22)))
//    // cf.simList().take(100).foreach(x => System.out.println("%s\t%s".format(x._1, x._2.map(r => r._1 + ":" + "%5f".format(r._2)))))
//    cf.simList().map(r => prefix + r._1 + "\t" + r._2.map(r => r._1 + ":" + "%5f".format(r._2)).take(100).reduce(_ + " " + _))
//      .saveAsTextFile(output + "/experiment")
//
////      .filter(item => total_user_uid_diu.contains(item._2)) //过滤是否uid可转diu
////      .map(item=> (item._1, total_user_uid_diu(item._2))) // (uid, uid2diu) uid转diu
////    println("total user follow data is")
////    println(rdd.count())
////    println(rdd.take(10).map(x => (x._1, x._2)))
////    val res2 = rdd.take(10).foreach(x => println("%s -> %s".format(x._1, x._2)))
//      .filter(item => userWatch.contains(item._2)) //过滤关注的用户是否有观看历史记录
//      .map(item => (item._1, userWatch(item._2))) // 获取diu观看历史记录
//
//
//
//      .flatMapValues(x => x) //uid 关注用户看的vid
//      .map{case(a,b) => ((a,b),1)}
//      .reduceByKey(_+_) //统计频次
//      .filter(item => item._2 > 1)  //频次大于1
//      .map(item => (item._1._1, (item._1._2, item._2)))
//      .groupByKey()
////      .filter(item => item._2.size > 10) //至少有10个结果
//      .map {
//        case (i, iter) =>
//          val mostplayed = iter.toArray.sortWith(_._2 > _._2).map(r => r._1 + ":" + "%d".format(r._2)).take(100).reduce(_ + " " + _)
//          prefix + i + "\t" + mostplayed
//      }.saveAsTextFile(output + "/follow_watch")
//    }
//
//}