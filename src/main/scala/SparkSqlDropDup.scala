import org.apache.spark.sql.{SaveMode, SparkSession}
object SparkSqlDropDup {

  def main(args: Array[String]): Unit = {
      //读取分区目录下的文件
      //.master("local[2]").appName("EmpETLV1App")
      val spark = SparkSession.builder().getOrCreate()

    val time = spark.sparkContext.getConf.get("spark.time")

    val day = time.substring(0,8)  // 20190608
    val hour = time.substring(8,10)  // 10

    val input = s"hdfs://localhost:8020/ruozedata/offline/emp/raw/$time"
    val output = s"hdfs://localhost:8020/ruozedata/offline/emp/col/day=$day/hour=$hour"

    var logDF = spark.read.format("text").load(input)





    logDF.write.option("compression","none").format("csv")
      .mode("overwrite").save(output)


    Thread.sleep(30000)

    spark.stop()
  }
}