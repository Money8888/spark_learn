package com.learn.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * 整合redis
 * 将词频统计结果存到redis中
 */
object NetworkWordCountToRedis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountToRedis").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("Centos1", 9999)
    val pairs: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

    // 保存数据到redis
    pairs.foreachRDD{ rdd => // 对每个RDD
      rdd.foreachPartition{ partitionOfRecords => // 对每个分区
        var jedis : Jedis = null
        // 采用单例模式
        try{
          jedis = JedisPoolUtil.getConnection
          // 循环每条记录
          partitionOfRecords.foreach(record => jedis.hincrBy("wordCount", record._1, record._2))
        }catch {
          case ex : Exception =>
            ex.printStackTrace()
        }finally {
          if(jedis != null){
            jedis.close()
          }
        }
      }
    }
    // 开启服务
    ssc.start()
    ssc.awaitTermination()
  }

}
