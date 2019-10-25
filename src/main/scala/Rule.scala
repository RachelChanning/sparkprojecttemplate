import com.rachel.templates.configurations.ConfigHelper
import com.rachel.templates.utils.{BoncLogging}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.sketch.BloomFilter

/**
 * @ClassName:Rule
 * @Description: TODO
 * @auther: Administrator
 * @date: 2019/9/30 003018:25
 * @Version 1.0
 */
object Rule extends  BoncLogging{
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      logError(s"""
                  |参数1:AppName(也作为消费者group id) <period>
                  |参数2:批次时间(毫秒) <period>
           """.stripMargin)
      System.exit(1)
    }
    val (appName,period) = (args(0),args(1))
    val conf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(Integer.parseInt(period.trim)))

    val spark = SparkSession
      .builder
      .getOrCreate()

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String]( ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](ConfigHelper.topics, ConfigHelper.kafkaParams))
    .map(x=>(x.value(),x.timestamp()))



      kafkaDirectStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        iter.foreach(x=> {
          if (filters.value.mightContain(x._1)) {
            println("contains" + x._1)
          } else {
            println("not contains" + x._1)
          }
        })

      })
      }
    )



    ssc.start()
    ssc.awaitTermination()
  }
}
