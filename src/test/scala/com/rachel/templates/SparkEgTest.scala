package com.rachel.templates

import com.rachel.templates.configurations.ConfigHelper
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.{After, Before, Test}

/**
 * @ClassName:SparkEgTest
 * @Description: TODO
 * @auther: Administrator
 * @date: 2019/9/30 003018:45
 * @Version 1.0
 */
@Test
class SparkEgTest {
  var ssc :StreamingContext= _
  @Before
  def createSparkContext (): Unit ={
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
//
//    ssc = new StreamingContext(conf, Seconds(5))
//    ssc.sparkContext.setLogLevel("ERROR")
  }

    @Test
    def testDstreamValue()={
//      val kafkaDirectStream = KafkaUtils.createDirectStream[String, String]( ssc,
//        LocationStrategies.PreferConsistent,
//        ConsumerStrategies.Subscribe[String, String](ConfigHelper.topics, ConfigHelper.kafkaParams))
//        .map(_.value()).cache()
//      kafkaDirectStream.foreachRDD(x=>{
//        println(x.count())
//      })
    }


  @After
  def startComput()={
//    ssc.start()
//    ssc.awaitTermination()
  }
}
