package com.learn.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器与广播变量
 */
object RDDAdvance {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark shell").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 累加器
    val accumData = Array(1,2,3,4,5)
    val accum = sc.longAccumulator("Accumulator")
    sc.parallelize(accumData).foreach(x => accum.add(x))
    // 获取累加器的值
    println(accum.value)

    // 广播变量
    // 不把副本变量分发到每个 Task 中，而是将其分发到每个 Executor，
    // Executor 中的所有 Task 共享一个副本变量
    // 把数组对象广播出去
    val broadcastVar = sc.broadcast(Array(1,2,3,4,5))
    // 使用时使用广播变量，不是用原值
    sc.parallelize(broadcastVar.value).map(_ * 10).collect().foreach(println)

  }
}
