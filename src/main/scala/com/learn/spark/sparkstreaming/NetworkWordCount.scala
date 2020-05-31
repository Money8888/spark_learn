package com.learn.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 利用socket读取网上的输入
 * 根据实时输入进行流处理
 * 需要服务器端开放端口
 * nc -lk 9999
 */
object NetworkWordCount {

  def main(args: Array[String]): Unit = {
    // 本地启动
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
    // Spark Streaming 编程的入口类是 StreamingContext
    // 批次拆分的时间间隔
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 创建socket文本流，进行词频统计
    val lines = ssc.socketTextStream("Centos1", 9999)
    // 只能统计每一次输入文本中单词出现的数量
    lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).print()

    // 启动服务
    ssc.start()
    // 等待服务结束
    ssc.awaitTermination()
  }

}
