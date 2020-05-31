package com.learn.spark.sparksql

import org.apache.spark.sql.SparkSession

/**
 * 使用反射生成dataFrame的字段
 */
object sparkSqlReflect {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL reflect example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // 隐式转化，包含了将RDD转化为DateFrame的操作
    import spark.implicits._

    val peopleDF = spark.sparkContext
      .textFile("src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // 创建临时视图
    peopleDF.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("select name, age from people where age between 13 and 19")
    teenagersDF.show()
  }
}
