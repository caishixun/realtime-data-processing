package com.hainiu.spark.redis_real

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object HainiuJedisConnectionPool {

  private val config = new JedisPoolConfig
  //最大的连接数
  config.setMaxTotal(20)

  //最大的空闲数
  config.setMaxIdle(10)

  //保持连接活跃
  config.setTestOnBorrow(true)


  //访问带密码的redis库
  //private val pool = new JedisPool(config,"nn1.hadoop",6379,10000,"hainiu666")
  private val pool = new JedisPool(config, "nn1.hadoop", 6379, 10000)

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn: Jedis = HainiuJedisConnectionPool.getConnection()
    conn.set("hainiu1", "1000")
    val r1: String = conn.get("hainiu1")

    println(r1)

    conn.incrBy("hainiu1", -50)

    val r2: String = conn.get("hainiu1")

    println(r2)

    val rs: util.Set[String] = conn.keys("*")

    import scala.collection.JavaConversions._
    for (r <- rs) {
      println(s"${r}:${conn.get(r)}")
    }
    conn.close()

    //客户端的使用
    //    val jedis = new Jedis("nn1.hadoop",6379,10000)
    //    jedis.auth("hainiu666")
    //    jedis.set("hainiu2","666")
    //    println(jedis.get("hainiu2"))
    //    jedis.close()
  }
}