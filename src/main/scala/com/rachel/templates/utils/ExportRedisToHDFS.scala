package com.rachel.templates.utils

import java.io.IOException

import com.rachel.templates.configurations.{ConfigHelper, JedisUtil}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * @ClassName:ExportRedisToHDFS
 * @Description: TODOcom.rachel.templates.utils.ExportRedisToHDFS
 * @auther: Administrator
 * @date: 2019/10/11 00119:47
 * @Version 1.0
 */
object ExportRedisToHDFS {
  case class UserInfo(phone:String,gender: String,ageLevel: String,homeLocation: String,homeLocationType: String,populationType: String)

  val jedisUtil = JedisUtil()

  def delRedisKey(spark: SparkSession): Unit = {
    jedisUtil.jedisFlow(x=>{
      val key = x.get("testa")
      println(key)
      x.expire("testa",1)
    })
  }


  def selectColumnUserInfo(spark: SparkSession): Unit = {
    spark.sql("SELECT  phone ,gender, ageLevel, homeLocation ,homeLocationType, populationType FROM parquet.`./dwa_v_d_cus_hk_user_info/*.parquet` limit 100000")
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("compression","none")
      .parquet("./dwa_v_d_cus_hk_user_info_selectColumn/")

  }


  def conventer050UserInfoToParquet(spark: SparkSession): Unit = {
    import spark.implicits._
    val oldDF = spark.sql("select * from text.`/user/hive/warehouse/dwa.db/dwa_v_d_cus_hk_user_info/`")

    oldDF.map(x=>x.getString(0).split("\\u0001")).map(x=>UserInfo(x(3),x(4),x(6),x(15),x(16),x(12)))
      .coalesce(2).write.format("parquet").mode(SaveMode.Overwrite)
      .option("compression","none")
      .save("/user/hive/warehouse/dwa.db/dwa_v_d_cus_hk_user_info_parquet/")
  }

  def conventer050SenceInfoToParquet(spark: SparkSession): Unit = {
    import spark.implicits._
    val oldDF = spark.sql("select * from text.`/user/hive/warehouse/dim.db/dim_lbs_spectacle_cell`")

    oldDF.map(x=>x.getString(0).split("\\u0001"))
      .map(x=>SenseInfo(x(0),x(1),x(2),x(3).toLowerCase))
      .coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .option("compression","none")
      .save("/user/hive/warehouse/dim.db/dim_lbs_spectacle_cell_parquet/")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ExportRedisToHDFS")
    val spark = SparkSession
    .builder
    .getOrCreate()
    conventer050SenceInfoToParquet(spark)

//    import org.apache.commons.net.ftp.FTP
//    import org.apache.commons.net.ftp.{FTPClient, FTPFile, FTPReply}
//
//    val ftp1 = new FTPClient
//      //host
//    val host = ConfigHelper.getFTPHost
//    val username = ConfigHelper.getFTPUserName
//    val password = ConfigHelper.getFTPpassword
//    val basePath = ConfigHelper.getFTPloadPath
//    try {
//      var reply = 0
//      ftp1.connect(host)
//      // 如果采用默认端口，可以使用ftp.connect(host)的方式直接连接FTP服务器
//      ftp1.login(username, password) // 登录
//      ftp1.enterLocalPassiveMode()
//      reply = ftp1.getReplyCode
//      if (!FTPReply.isPositiveCompletion(reply)) {
//        ftp1.disconnect()
//      }
//      ftp1.disconnect()
//
//      ftp1.changeWorkingDirectory(basePath)
//
//    }catch {
//      case e: IOException =>
//        e.printStackTrace()
//    } finally {
//      if (ftp1.isConnected) try
//        ftp1.disconnect()
//      catch {
//        case e: IOException =>println(e)
//      }
//    }
  }


  def exportUserHiveInfo(spark:SparkSession): Unit ={
    val resource = jedisUtil.getJedis()
//    val test = resource.hkeys("test")
    spark.read.parquet("./dwa_v_d_cus_hk_user_info/").collect()
      .foreach(y=>{
        val hashMap = new java.util.HashMap[String,String]()
//        hashMap.put("1","2")
        hashMap.put(y(0).toString,y(1).toString)
        resource.hmset("test",hashMap)
      })
    resource.close()
//      .foreach(x=>{
//
//      })
    //homeLocationType 省内，省外
    //populationType 海口户籍，旅游人口，本地户籍人口，常住人群，候鸟人口,旅游人群,候鸟人群,常住人口(非海口户籍),其他

  }

  def exportSenseInfo(spark:SparkSession): Unit ={
    val spectkeys = new ArrayBuffer[SenseInfo]();
    jedisUtil.jedisFlow(x=>{
      val keys =  x.keys("spectacle|*")
      val iterator = keys.iterator()
      while (iterator.hasNext){
        val keys = iterator.next()
        val value = x.get(keys).split("\\|")
        val lac_Ci = keys.split("\\|")(1).split("-")
        spectkeys += SenseInfo(lac_Ci(0),lac_Ci(1),value(0),value(1).toLowerCase())
      }
    })

    import  spark.implicits._
    spectkeys.toDF("lac","ci","sceneNames","sceneId")
    .coalesce(1)
    .write
    .option("compression","none")
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("/user/hive/warehouse/dim.db/DIM_PUB_SEPCTACLE_CELL/")
  }

  /**
   * @Author longmeijuan
   * @Description //TODO 导出用户数据
   * @Date 11:51 2019/10/11 0011
   * @Param [args]
   * @return void
   **/
  def exportUserInfo(spark:SparkSession): Unit ={
    val userInfos = new ArrayBuffer[UserInfo]();
    jedisUtil.jedisFlow(x=>{
      val keys =  x.keys("user|*")
      val iterator = keys.iterator()
      while (iterator.hasNext){
        val key = iterator.next()
        val value = x.get(key).split("\\|")
        val phone = key.split("\\|")(1)
        userInfos += UserInfo(phone,value(0),value(1),value(2),value(3),value(4))
      }
    })
    import  spark.implicits._
    userInfos.toDF("phone","gender","ageLevel","homeLocation","homeLocationType","populationType")
//      .show(200)
      .coalesce(2)
      .write
      .option("compression","none")
      .format("parquet")
      .mode(SaveMode.Append)
      .save("/RedisUserInfoFiles")
  }
  case class SenseInfo(lac: String,ci: String,sceneNames: String,sceneId: String)
}

