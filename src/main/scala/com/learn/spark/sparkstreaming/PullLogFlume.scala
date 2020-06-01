package com.learn.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 拉取式方法
 * 更强的可靠性和容错保证
 * a1.sinks.k1.type = org.apache.spark.streaming.flume.sink.SparkSink
 */
object PullLogFlume {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val flumeStream = FlumeUtils.createPollingStream(ssc, "Centos2", 9999)
    flumeStream.map(line => new String(line.event.getBody.array()).trim).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
