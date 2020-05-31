package com.learn.spark.sparksql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class Employee(name: String, salary: Long, age: Long)
case class Average(var sum: Long, var count: Long)
object MyAverage extends Aggregator[Employee, Average, Double]{

  // 初始值
  def zero: Average = Average(0L, 0L)

  // 最后一行为返回值，返回buffer
  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  // 合并
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  // 指定存在缓存中的数据类型
  def bufferEncoder: Encoder[Average] = Encoders.product

  // 指定输出结果的数据类型
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object main{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Dataset example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // 隐式转化，包含了将RDD转化为DateFrame的操作
    import spark.implicits._
    val ds = spark.read.json("src/main/resources/employees.json").as[Employee]
    ds.show()

    // 用自定义的函数进行平均值计算
    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
  }
}