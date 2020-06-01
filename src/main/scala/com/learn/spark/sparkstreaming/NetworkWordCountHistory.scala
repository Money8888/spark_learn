package com.learn.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 统计所有历史输入中单词出现的数量
 * 使用updateStateByKey
 */
object NetworkWordCountHistory {

  def main(args: Array[String]): Unit = {

    // 设置hdfs目录的用户，防止出现权限不足
    System.setProperty("HADOOP_USER_NAME", "root")

    val sparkConf = new SparkConf().setAppName("NetworkWordCountHistory").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 设置检查点
    // 当使用 updateStateByKey 算子时，它会去检查点中取出上一次保存的信息，
    // 并使用自定义的 updateFunction 函数将上一次的数据和本次数据进行相加，然后返回
    ssc.checkpoint("hdfs://Centos1:8020/spark-streaming")
    // 监听主机的socket进程
    val lines = ssc.socketTextStream("Centos1", 9999)
    lines.flatMap(_.split(" ")).map(x => (x, 1))
      .updateStateByKey[Int](updateFunction _)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }

}
