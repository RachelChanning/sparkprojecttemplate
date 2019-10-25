package com.rachel.templates.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Author longmeijuan
 * @Description //TODO
 * @Date 20:14 2019/9/27 0027
 * @Param
 * @return
 **/
object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      synchronized {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
    }
    instance
  }
}