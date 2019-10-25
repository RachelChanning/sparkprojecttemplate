package com.rachel.templates.configurations

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @ClassName:ConfigHelper
 * @Description: TODO
 * @auther: Administrator
 * @date: 2019/9/29 002918:42
 * @Version 1.0
 */
object ConfigHelper {

  /**
     * 加载 mysql配置
     */
//    DBs.setup()

    /**
     * 加载application.conf文件
     */
    private lazy val application: Config = ConfigFactory.load()

    /**
     * kafka Servers
     */
    val brokers = application.getString("kafka.brokers")

    /**
     * groupId
     */
    val groupId = application.getString("kafka.groupId")

    /**
     * topics
     */
    val topics = application.getString("kafka.topics").split(",")

    /**
     * redis host
     */
    val redisHost = application.getString("redis.host")

    /**
     * redis port
     */
    val redisPort = application.getInt("redis.port")

    /**
     * redis timeout
     */
    val redisTimeout = application.getInt("redis.timeout")


    /**
      * redis databaseIndex
    */
    var databaseIndex: Int = application.getInt("redis.batabase.index")


  /**
     * kafka params
     */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigHelper.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "max.poll.records" -> "500",
      "heartbeat.interval.ms" -> "30000",
      "session.timeout.ms" -> "90000",
      "request.timeout.ms" -> "95000",
      "group.max.session.timeout.ms" -> "300000",
      "batch.size"->"1000",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /**
    *36个场景
    */
   val senseIds = "wrhx"    :: "wyh" :: "xyg"  :: "rygc" :: "hsk"    :: "nmxcj"::
    "wly"    :: "hkg" :: "ydgc" :: "wdgc" :: "yxgc"   :: "whgj" ::
    "wgc"    :: "gxc" :: "dygs" :: "jnl"  :: "sjgy"   ::"jhc"   ::
    "xcwy"   :: "ng"  :: "xzl"  :: "hcz"  ::"bhgy"    :: "hcdz" ::
    "yygctlc"::"hzzx" :: "qllj" :: "mljc" :: "xhg"    :: "jrht" ::
    "bsm"    :: "jfx" :: "qcxz" :: "hkys" :: "sbbhc"  :: "gly"  ::Nil
//    val pcode2nameDictMap = application.getObject("pcode2pname").unwrapped()

  val  getFTPHost = application.getString("FTP.host")
  val getFTPUserName = application.getString("FTP.username")
  val getFTPpassword = application.getString("FTP.password")
  val getFTPloadPath = application.getString("FTP.loadPath")
}
