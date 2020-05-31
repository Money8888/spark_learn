package com.leran.spark.rdd

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
//  val data = Array(1, 2, 3, 4, 5)

/**
 *  IDEA 中以本地模式运行 Spark 项目是不需要在本机搭建 Spark 和 Hadoop 环境的
 */
object RDD {
  def main(args: Array[String]): Unit = {
    // 加载配置,构造变量sc 相当于执行了spark-shell --master local[4]
    //  val ： 类似于 Java 中的 final 变量，一旦初始化就不能被重新赋值；
    //  var ：类似于 Java 中的非 final 变量，在整个声明周期内 var 可以被重新赋值
    val conf = new SparkConf().setAppName("Spark shell").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    // 由现有集合创建 RDD,默认分区数为程序所分配到的 CPU 的核心数
    // val dataRDD = sc.parallelize(data)
    // 指定的分区
    val dataRDD = sc.parallelize(data, 2)
    // 查看分区数
    // 4
    println(dataRDD.getNumPartitions)

    println("+++++++++++++++++++++++++++++++++++++")
    // 读取外部数据源转化成RDD，根据core-site.xml来指定
    // val fileRDD = sc.textFile("D:\\JavaProject\\spark_learn\\tip.txt")
    // println(fileRDD.take(1))

    // RDD操作
    val list = List(1,2,3)
    // sc.parallelize(list)转化为RDD
    // map 为转换操作，_ 表示元素
    // foreach为action操作，后面传入函数
    // 输出顺序不与list预期顺序一致
//    val listRDD = sc.parallelize(list)
//    println(listRDD.getStorageLevel)
//    println("----------------------------")
//    println(listRDD.getNumPartitions)
//    listRDD.map(_ * 10).foreach(println)
    sc.parallelize(list).map(_ * 10).foreach(println)
    dataRDD.persist(StorageLevel.MEMORY_AND_DISK)
    //dataRDD.cache()
    dataRDD.unpersist()
    dataRDD.cache()
  }
}
