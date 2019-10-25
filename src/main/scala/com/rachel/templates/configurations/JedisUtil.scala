package com.rachel.templates.configurations

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @ClassName:com.test.JedisUtil
 * @Description: TODO
 * @auther: Administrator
 * @date: 2019/9/29 002919:11
 * @Version 1.0
 */
class JedisUtil(ip:String, port:Int, timeout:Int,dataBaseIndex:Int){

  private var pool: JedisPool = null
  /**
   * 初始化jedis连接池
   */
  def poolInit() = {
    val config: JedisPoolConfig = new JedisPoolConfig
    val jedisPool = new JedisPool(config, ip, port, timeout,null,dataBaseIndex)
    pool = jedisPool
  }

  /**
   * 获取jedis连接
   * @return
   */
  def getJedis() = {
    if(pool == null){
      poolInit()
    }
    pool.getResource
  }

  /**
   * jedis业务流程方法
   * @param op
   */
  def jedisFlow(op: Jedis => Unit): Unit ={
    var jedis: Jedis = null
    try{
      jedis = getJedis()
      op(jedis)
    }catch{
      case e: Exception => e.printStackTrace()
    }finally {
      jedis.close()
    }
  }

  /**
  * @Author longmeijuan
  * @Description //TODO 关闭redi连接
  * @Date 9:45 2019/9/30 0030
  * @Param
  * @return
  **/
  def returnResource(redis: Jedis) = {
    if (redis != null) redis.close()
  }


}

object JedisUtil {
  def apply(): JedisUtil = new JedisUtil(ConfigHelper.redisHost, ConfigHelper.redisPort, ConfigHelper.redisTimeout,ConfigHelper.databaseIndex)

  def main(args: Array[String]): Unit = {
  }
}