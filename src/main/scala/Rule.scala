import com.rachel.templates.configurations.ConfigHelper
import com.rachel.templates.utils.{BoncLogging, SparkSessionSingleton}
import org.apache.spark.SparkConf

/**
 * @ClassName:Rule
 * @Description: TODO
 * @auther: Administrator
 * @date: 2019/9/30 003018:25
 * @Version 1.0
 */
object Rule extends BoncLogging {


  def main(args: Array[String]): Unit = {
    val session = SparkSessionSingleton.getInstance(new SparkConf().setMaster("local[2]"))
    val DS = session.sparkContext.textFile("./src/main/resources/红楼梦.txt")
      .map(_.replaceAll("．", "，"))
      .map(_.replaceAll("\"", "，"))
      .map(_.replaceAll("“", "，"))
      .map(_.replaceAll("”", "，"))
      .map(_.replaceAll("；", "，"))
      .map(_.replaceAll("！", "，"))
      .map(_.replaceAll("？", "，"))
      .map(_.replaceAll("。", "，"))
      .map(_.split("第一回")(0))
      .map(x => {
        (x.split("，"), x.split("，").length - 1)
      }).map(x => {
      /***
      金陵12钗
       林黛玉、薛宝钗、贾元春、贾探春、史湘云、妙玉、贾迎春、贾惜春、王熙凤、贾巧姐、李纨、秦可卿
       **/
      var index = 0
      var daiyuCount = 0
      for (index <- 0 to x._2) {
        //判断字段中是否包含了指定数据。
        try {
          if (x._1(index).contains("黛玉")||x._1(index).contains("林姑娘")||x._1(index).contains("林丫头")) {
            daiyuCount += 1
          }
        } catch {
          case e: Exception => {
            println(x._2 + "-" + index)
            e.printStackTrace()
          }
        }
      }
      ("黛玉", daiyuCount)
    }).reduceByKey(_ + _).foreach(println(_))

  }

  case class Record(value:String)
}
