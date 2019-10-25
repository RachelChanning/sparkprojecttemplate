# sparkprojecttemplate
## 这是一个spark streaming的模板程序
Rule类为写业务代码的主类

工具类中包含的功能有如下几点（可以在Rule类中调用）：
1：监听器，在spark streaming中注册监听器达到监控目的

2：日志基础类，所有的业务程序都继承自BoncLogging该日志类，统一管理日志

3: redis/hive中的数据导出到HDFS，从txt到Parquet格式
PS:此处需要在resource目录下添加hive-site.xml,以及core-site.xml,hdfs-site.xml文件并且sparksession开启了enablehiveSupport()才能正常使用

4: kafka集成工具类，Spark streaming从kafka topic中拉取数据，以及将Spark Streaming处理完的数据发往下游kafka的工具类集成

5: Spark streaming程序中调用spark sql(SparkSession)

6: Spark Streaming集成redis数据库的读写操作

7: 配置项都在配置文件中进行配置，而不是硬编码，配置文件为：application.conf

8: 项目resource目录下包含Log4j文件，用于本地调试控制IDEA的控制台输出