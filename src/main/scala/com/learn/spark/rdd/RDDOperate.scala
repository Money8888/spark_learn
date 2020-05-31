package com.learn.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * 常用的操作
 */
object RDDOperate {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark shell").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // transformations惰性求值

    // filter,过滤大于10的
    val filterList = List(3, 6, 9, 10, 12, 21)
    sc.parallelize(filterList).filter(_ >= 10).foreach(println)

    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    // flatMap,将若干个小集合映射成大集合
    val flatMapList = List(List(1,2), List(3), List(), List(4, 5))
    sc.parallelize(flatMapList).flatMap(_.toList).map(_*10).foreach(println)
    // 类比日志分析
    val flatMapLines = List("spark flume spark", "hadoop flume hive")
    // 将每个元素按照空格分开并计数，然后相同的word进行计数求和
    sc.parallelize(flatMapLines).flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).foreach(println)

    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    // mapPartitions函数，与 map 类似，但函数单独在 RDD 的每个分区上运行,函数输入和输出都必须是可迭代类型
    val mapPartitionsList = List(1,2,3,4,5,6)
    sc.parallelize(mapPartitionsList, 3).mapPartitions(iter => {
      // 分区上新建缓冲
      val buffer = new ListBuffer[Int]
      while (iter.hasNext){
        // 写入数据
        buffer.append(iter.next() * 100)
      }
      // 把各个分区的缓冲改变成可迭代的
      buffer.toIterator
    }).foreach(println)

    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    // mapPartitionsWithIndex,第一个参数为分区索引
    val mapPartitionsWithIndexList = List(1,2,3,4,5,6)
    sc.parallelize(mapPartitionsWithIndexList, 3).mapPartitionsWithIndex((index, iter) =>{
      val buffer = new ListBuffer[String]
      while(iter.hasNext){
        buffer.append(index + "分区:" + iter.next() * 100)
      }
      buffer.toIterator
    }).foreach(println)

    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++")

    // sample 数据采样 设置是否放回 (withReplacement)、采样的百分比 (fraction)、随机数生成器的种子 (seed)
    val sampleList = List(1,2,3,4,5,6)
    sc.parallelize(sampleList).sample(withReplacement = false, fraction = 0.5, seed = 1).foreach(println)

    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
    // union 合并两个RDD
    val unionList1 = List(1, 2, 3)
    val unionList2 = List(4, 5, 6)
    sc.parallelize(unionList1).union(sc.parallelize(unionList2)).foreach(println)
    // intersection交集
    sc.parallelize(unionList1).union(sc.parallelize(unionList2)).intersection(sc.parallelize(List(2,3,7)))
    // 去重 distinct()
    sc.parallelize(List(1,1,2,3,4,4)).distinct().foreach(println)

    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    // groupByKey按键进行分组
    val groupByKeyList = List(("hadoop", 2), ("spark", 3), ("spark", 5), ("storm", 6), ("hadoop", 2))
    sc.parallelize(groupByKeyList).groupByKey().map(x => (x._1, x._2.toList)).foreach(println)
    sc.parallelize(groupByKeyList).reduceByKey(_ + _).foreach(println)

    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    // sortBy(按照指定元素排序) & sortByKey(按照指定键排序)
    val sorList1 = List((100, "hadoop"), (90, "spark"), (120, "storm"))
    val sorList2 = List(("hadoop",100), ("spark",90), ("storm",120))
    sc.parallelize(sorList1).sortByKey(ascending = false).foreach(println)
    sc.parallelize(sorList2).sortBy(x => x._2, ascending = false).foreach(println)

    println("++++++++++++++++++++++++++++++++++++++++++++++++++++")

    // RDD之间的联结操作
    // join(leftOuterJoin, rightOuterJoin 和 fullOuterJoin), cogroup, cartesian
    // join根据键进行内连接操作，返回 (K, (V, W)) 的 Dataset
    val joinList1 = List((1, "student01"), (2, "student02"), (3, "student03"))
    val joinList2 = List((1, "teacher01"), (2, "teacher02"), (3, "teacher03"))
    sc.parallelize(joinList1).join(sc.parallelize(joinList2)).foreach(println)

    // cogroup
    // out: 返回多个类型为 (K, (Iterable<V>, Iterable<W>)) 的元组所组成的 Dataset
    // (2,(CompactBuffer(b),CompactBuffer(B),CompactBuffer([bB])))
    // (1,(CompactBuffer(a, a),CompactBuffer(A),CompactBuffer([ab])))
    // (3,(CompactBuffer(e),CompactBuffer(E),CompactBuffer(eE, eE)))
    val cogroupList01 = List((1, "a"),(1, "a"), (2, "b"), (3, "e"))
    val cogrouplist02 = List((1, "A"), (2, "B"), (3, "E"))
    val cogroupList03 = List((1, "[ab]"), (2, "[bB]"), (3, "eE"),(3, "eE"))
    sc.parallelize(cogroupList01).cogroup(sc.parallelize(cogrouplist02), sc.parallelize(cogroupList03)).foreach(println)

    // cartesian笛卡尔积,两两自由组合
    val cartesianList1 = List("A", "B", "C")
    val cartesianList2 = List(1, 2, 3)
    sc.parallelize(cartesianList1).cartesian(sc.parallelize(cartesianList2)).foreach(println)

    println("+++++++++++++++++++++++++++++++++++++++++++")

    // action

    // reduce
    val reduceList = List(1,2,3,4,5)
    // println(sc.parallelize(reduceList).reduce((x, y) => x + y))
    println(sc.parallelize(reduceList).reduce(_ + _))

    // takeOrdered返回排序后的元素，takeOrdered 使用隐式参数进行隐式转换

    // 自定义排序类,根据英文单词长度进行排序
    class CustomizeOrder extends Ordering[(Int, String)]{
      override def compare(x: (Int, String), y: (Int, String)): Int
      = if(x._2.length > y._2.length) 1 else -1
    }

    val takeOrderedList = List((1, "hadoop"), (1, "storm"), (1, "azkaban"), (1, "hive"))
    // 引入隐式默认值
    implicit val implicitOrder: CustomizeOrder = new CustomizeOrder
    sc.parallelize(takeOrderedList).takeOrdered(5).foreach(println)

    // countByKey计算每个键的次数
    val countByKeyList = List(("hadoop", 10), ("hadoop", 10), ("storm", 3), ("storm", 3), ("azkaban", 1))
    sc.parallelize(countByKeyList).countByKey().foreach(println)
    // saveAsTextFile,dataset 中的元素以文本文件的形式写入本地文件系统、HDFS 或其它 Hadoop 支持的文件系统中
  }

}
