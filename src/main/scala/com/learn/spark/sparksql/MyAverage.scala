package com.learn.spark.sparksql

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

case class Employee(name: String, salary: Long, age: Long)
case class Average(var sum: Long, var count: Long)

// 有类型自定义函数
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

object MyAverageNoType extends UserDefinedAggregateFunction{
  // 聚合操作输入参数的类型
  override def inputSchema: StructType = StructType(StructField("InputColumn", LongType) :: Nil)
  // 聚合操作中间值的类型
  override def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  // 聚合操作输出参数的类型
  override def dataType: DataType = DoubleType

  // 函数是否始终在相同输入上返回相同的输出,通常为 true
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(0) = 0L
  }

  // 同一分区中的 reduce 操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 不同分区中的 merge 操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终的输出值
  override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
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

    // 用无类型的函数进行计算
    // 注册函数
    spark.udf.register("myAverageNoType", MyAverageNoType)
    // 创建临时表
    val df = spark.read.json("src/main/resources/emp.json")
    df.createOrReplaceTempView("emp")
    val myAvg = spark.sql("SELECT myAverageNoType(sal) as avg_sal FROM emp")
    val avg = spark.sql("SELECT avg(sal) as avg_sal FROM emp")
    myAvg.show()
    avg.show()
  }
}