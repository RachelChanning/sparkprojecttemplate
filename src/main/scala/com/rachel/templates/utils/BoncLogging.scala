package com.rachel.templates.utils

/**
 * @ClassName:boncLogging
 * @Description: TODO
 * @auther: Administrator
 * @date: 2019/9/27 002721:16
 * @Version 1.0
 */
class BoncLogging extends org.apache.spark.internal.Logging {

  override def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn("com.bonc => "+msg)
  }

  override def logError(msg: => String) {
    if (log.isErrorEnabled) log.error("com.bonc => "+msg)
  }

}
