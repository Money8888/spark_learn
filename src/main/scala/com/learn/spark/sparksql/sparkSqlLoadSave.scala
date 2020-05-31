package com.learn.spark.sparksql

import org.apache.spark.sql.SparkSession

/**
 * 导入导出
 */
object sparkSqlLoadSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL load/save example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // 隐式转化，包含了将RDD转化为DateFrame的操作
    import spark.implicits._
    /**
     * 读取与写入parquet文件
     * usersDF
     * +------+--------------+----------------+
     * |  name|favorite_color|favorite_numbers|
     * +------+--------------+----------------+
     * |Alyssa|          null|  [3, 9, 15, 20]|
     * |   Ben|           red|              []|
     * +------+--------------+----------------+
     */
      /*
    val usersDF = spark.read.load("src/main/resources/users.parquet")
    // 将视图保存成parquet文件
    usersDF.select("name", "favorite_numbers").write.save("src/main/resources/namesAndFavColors.parquet")
      */
      // 直接读取parquet文件，不用导入保存
    val sqlDF = spark.sql("select * from parquet.`src/main/resources/namesAndFavColors.parquet`")
    sqlDF.show()

    // 读取csv文件
      /*
    val peopleDFCsv = spark.read.format("csv")
        .option("sep", ";")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("src/main/resources/people.csv")
    peopleDFCsv.show()
       */
  }

}
