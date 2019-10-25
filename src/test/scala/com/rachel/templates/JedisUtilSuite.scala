package com.rachel.templates

import com.rachel.templates.configurations.ConfigHelper
import org.junit.Test

/**
 * @ClassName:JedisUtilUnit
 * @Description: TODO
 * @auther: Administrator
 * @date: 2019/9/30 003010:46
 * @Version 1.0
 */
@Test
class JedisUtilSuite {
  import org.junit.Test
    @Test
    def hel:Unit = {
      val someValue = true
      assert(someValue == true)
    }

   @Test
  def testLpush :Unit={
    val jedis = JedisUtil()
     jedis.jedisFlow(jedis=>{
       jedis.lpush("sparkAPP_cons_cxdr", "2019-09-30 17:13:00|2436806|0|0|0");
       val length = jedis.llen("sparkAPP_cons_cxdr")
       println(length)
//       if (length >= duration) {
//         jedis.rpop(key);
//       }
     })
  }
}
