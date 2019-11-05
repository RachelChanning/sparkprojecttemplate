package com.rachel.templates.utils


import java.util.Date

import com.rachel.templates.configurations.JedisUtil
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted, StreamingListenerBatchSubmitted, StreamingListenerReceiverError, StreamingListenerReceiverStarted, StreamingListenerReceiverStopped}
import org.slf4j._


class BoncStreamingListener(private val appName: String, private val duration: Int,spark:SparkSession) extends SparkListener  with StreamingListener{

  private var refreshBrocastTime = new Date
  private var refreshSenceInfoTime = new Date
  private val logger = LoggerFactory.getLogger("BoncListener")
  private val jedisCon = JedisUtil()
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    super.onReceiverStarted(receiverStarted)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = super.onReceiverError(receiverError)

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = super.onReceiverStopped(receiverStopped)


  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    val batchInfo = batchStarted.batchInfo
    val processingStartTime = batchInfo.processingStartTime
    logger.info("BoncListener  processingStartTime : ", processingStartTime)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchInfo = batchCompleted.batchInfo
    val processingDelay = batchInfo.processingDelay.get
    val totalDelay = batchInfo.totalDelay.get
    val numRecords = batchInfo.numRecords
    val schedulingDelay = batchInfo.schedulingDelay.get
    val batchTime = DateFormatUtils.format(batchInfo.batchTime.milliseconds,"yyyy-MM-dd HH:mm:ss")
    val key = "sparkAPP_" + appName
    val value = (batchTime,numRecords,schedulingDelay,processingDelay,totalDelay).productIterator.mkString("|")
    jedisCon.jedisFlow(jedis=>{
      jedis.lpush(key, value)
      val length = jedis.llen(key)
      if (length >= duration) {
        jedis.rpop(key);
      }
    })

    /***
    * @Author longmeijuan
    * @Description //TODO 进行一些用户资料表的更新操作。
    **/
    // 20190424 定时10分钟刷新一次LteExpoSector
    val executeTime = new Date
    if(executeTime.getTime - refreshBrocastTime.getTime > 10 * 60 * 1000) {
      ExportRedisToHDFS.conventer050UserInfoToParquet(spark)
      refreshBrocastTime = executeTime
      logger.error("==============i  have execute the update of dimTable=======================")
    }


    /***
     * @Author longmeijuan
     * @Description //TODO 定时更新一次场景信息
     **/
    if(executeTime.getTime - refreshSenceInfoTime.getTime > 7 * 24 * 60 * 60 * 1000) {
      //更新广播变量
      ExportRedisToHDFS.conventer050SenceInfoToParquet(spark)
      refreshSenceInfoTime = executeTime


      logger.error("==============i  have execute the update of dimTable=======================")
    }



  }

}