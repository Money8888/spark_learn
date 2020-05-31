package com.learn.spark.sparksql
import org.apache.spark.sql.SparkSession


object sparkSqlJsonToDataset {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL jsonToDataset example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // 隐式转化，包含了将RDD转化为DateFrame的操作
    import spark.implicits._

    // 由json转df
    val df = spark.read.json("src/main/resources/employees.json")
    // 展示全表
    df.show()
    println("+++++++++++++++++++++++++++++++++++++++++")
    // 打印列名
    df.printSchema()
    println("+++++++++++++++++++++++++++++++++++++++++")
    // 只展示某列
    df.select("name").show()
    println("+++++++++++++++++++++++++++++++++++++++++")
    // 对某列的数进行+1操作
    df.select($"name", $"salary" + 1000).show()
    println("+++++++++++++++++++++++++++++++++++++++++")
    // 只显示age大于21的记录
    df.filter($"salary" > 5000).show()
    println("+++++++++++++++++++++++++++++++++++++++++")
    // 按年龄分组求和
    df.groupBy("age").count().show()

    // 创建临时视图
    // 只能在一个sql会话中用
    /*df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()*/

    // 创建一个全局视图
    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()
    // 开启另一个sql会话
    spark.newSession().sql("select * from global_temp.people").show()


  }

}
