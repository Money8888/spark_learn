package com.learn.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 监听式整合flume捕获日志信息
 */
object LogFlumeListen {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 获取输入流
    val flumeStream = FlumeUtils.createStream(ssc, "Centos2", 9999)
    // 打印输入流的消息
    flumeStream.map(line => new String(line.event.getBody.array()).trim).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
