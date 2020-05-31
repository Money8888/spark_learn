package com.learn.spark.sparksql

import org.apache.spark.sql.SparkSession

object sparkSqlDataset {

  // 人的实体类
  case class Person(name: String, salary: Double, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Dataset example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // 隐式转化，包含了将RDD转化为DateFrame的操作
    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 4500, 32)).toDS()
    caseClassDS.show()

    // implicits自动匹配类型
    val implicitsDS = Seq(1,2,3).toDS()
    println(implicitsDS.map(_ + 1).collect())

    // 转化成指定的DataSet类
    val peopleDS = spark.read.json("src/main/resources/employees.json").as[Person]
    peopleDS.show()

    // 使用反射指定类


  }

}
