
package com.wifi.recommend

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.td.ml.xdata.common.{UserHistory, VideoProfile}
import com.wifi.recommend.utils.DataUtils.{getPreviousdate, transformTime}
import org.apache.parquet.it.unimi.dsi.fastutil.Arrays
import org.apache.spark.sql.SparkSession
import org.dmg.pmml.True
import scala.util.Random

import scala.util.control.Exception.allCatch
import scala.::
import scala.collection.mutable

/** .
 * Created by:jiangcx
 * Date: 2021/8/23
 */
object UsercfFollow {

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
    for (i <- 0 until math.min(arrayRaw.size(), 100)) {
      val item = arrayRaw.getJSONArray(i)
      if (item.getString(0) != ""){
        output += item.getString(0)
      }
    }
    output.toArray
  }
  def isFloat(s: String): Boolean = (allCatch opt s.toFloat).isDefined


  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      Console.err.println("input_user_follow, output, prefix, fromDate, toDate, input_user_history")
    }

    val Array(input_user_follow, output, prefix, fromDate, toDate, input_user_history) = args

    val spark = SparkSession
      .builder()
      .appName("userfollow")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://10.42.31.63:8020")
      .getOrCreate()

    val fromdt = new SimpleDateFormat("yyyy-MM-dd").parse(fromDate).toInstant.toEpochMilli  //传入
    val one_month = getPreviousdate(toDate,30, patten = "yyyy-MM-dd")// 30天前
    val one_month_ts = new SimpleDateFormat("yyyy-MM-dd").parse(one_month).toInstant.toEpochMilli
    val ctype_used =  List(501,502,503,103,105,107,109,111,112)

// ctr info
//    val vid_ctr_path = s"hdfs://10.42.31.63:8020/dw/adm/f_app_vid_user_all/dt=$toDate/000000_0.gz"
//    val vid_ctr = spark.read.option("sep", "\001").csv(vid_ctr_path).select("_c1", "_c22")
//      .rdd.map(r => (r.getString(0), r.getString(1))).filter(item => isFloat(item._2)).map(i=>(i._1,i._2.toFloat))//23万数据


    // users' words
    val path = s"/user/jiaxj/Apps/DocsInfo/$toDate/*"
    val vid_uid = spark.sparkContext.textFile(path).map { // cstatus == 0 and ctype in (103,105,107) and createtime <= 1 month and firstcat=264
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
        if ((firstcat==264) && (cstatus==0) && ctype_used.contains(ctype) && createtime>one_month_ts) {
          (vid, uid)
        } else {
          null
        }
    }.filter(item => item != null).cache()
//    val recalls = vid_uid.join(vid_ctr).map(item=> (item._2._1, (item._1, item._2._2))) //vid, uid, ctr
////      .groupByKey()
//      .filter(i => i._2._2>= 0.1) //按照ctr过滤
//      .groupByKey().map(i => (i._1, i._2.toArray.sortWith(_._2 > _._2).take(100))) //每个用户最多N的作品
//      .collectAsMap() //all users' words with ctr  ... (uid, [(vid, ctr)...])
//    println("recalls")
//    println(recalls.size)

    val recalls = vid_uid.map(item=> (item._2, item._1)) //uid, vid
      .groupByKey().map(i => (i._1, Random.shuffle(i._2))) //每个用户随机最多N的作品
      .collectAsMap() //all users' words with ctr  ... (uid, [vid, vid, ...])
    println("recalls")
    println(recalls.size)
    // #################################################  user ###############################################################################################
    val userpath = s"hdfs://10.42.31.63:8020/olap/db/user/dt=$toDate"
    val active_user = spark.sparkContext.textFile(userpath).map {
      line =>
        val cols = line.split('\u0001')
        var signdate = Long.MinValue
        try {
          signdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(cols(15)).toInstant.toEpochMilli
        } catch {
          case _: Throwable => 0
        }
        if (signdate>=one_month_ts ) {  //一个月内激活的用户
          cols(0)
        } else {
          null
        }
    }.filter(uid => uid != null).map(uid => (uid, 1)).collectAsMap() // (uid, 1)
    println("active users:")
    println(active_user.size)

    //uid2diu字典
    val total_user_uid_diu = spark.sparkContext.textFile(userpath).map {
      line =>
        val cols = line.split('\u0001')
        (cols(0),cols(33)) // (uid, diu)
    }.collectAsMap()
    //diu2uid字典
    val total_user_uid_diu2 = spark.sparkContext.textFile(userpath).map {
      line =>
        val cols = line.split('\u0001')
        (cols(33),cols(0)) // (diu, uid)
    }.collectAsMap()

    //
    val test_user_uid = spark.sparkContext.textFile(s"hdfs://10.42.31.63:8020/user/jiangcx/out.txt")
      .filter(item=>total_user_uid_diu2.contains(item.trim()))
      .map(item=> (total_user_uid_diu2(item.trim()),1)).collectAsMap() // diu to uid (uid, 1)
    println("white users have uid:")
    println(test_user_uid.size)

    val rdd = spark.sparkContext.textFile(input_user_follow)
      .map{ line => (line.split("\t")(0) ,loadRealFollows(line.split("\t")(1)) )} // uid, uids
      //      .filter(item => item._2.size != 0) //过滤是否为激活用户
      .filter(item => active_user.contains(item._1)).cache() //过滤是否为激活用户(一个月内登录的用户) uid
//      .filter(item => test_user_uid.contains(item._1)).cache() // 测试过滤  可以删除
    println("after filter active user")
    println(rdd.count())
    val rdd2 = rdd.filter(item => item._2.size >= 10).map(item => (item._1, item._2.take(500)))  //关注列表个数大于20,且最多取500
      .flatMapValues(x => x) // (uid,关注uid)

    val cf2 = new CollaborativeFiltering_usercf_follow(rdd2,Similarity.COSINE)
    cf2.userRecommend.filter(i=> i._2 != null).filter(i => test_user_uid.contains(i._1))
      .map(item => (item._1 , item._2.filter(uid=>recalls.contains(uid._1)).map(item => recalls(item._1)).flatMap(_.toList)))
       // .take(10).foreach(x => println("%s -> %s".format(x._1, x._2.toList.mkString(";"))))
       // .map(item => (item._1, item._2.map(i => i._1))) // (uid, [uid,uid,uid...uid])   // item2.size = 100
      .map(r => prefix + r._1 + "\t" + r._2.take(500).mkString(" "))
      .saveAsTextFile(output + "/userfollow")
    //(uid, 推荐uids)
    //获取所有uid的作品，并且按照ctr排序

  }
}