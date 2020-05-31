package com.learn.spark.sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * RDD， dataFrame，DataSet转化
 */
object sparkSqlConvert {
  case class Emp(ename: String, comm: Double, deptno: Long, empno: Long, hiredate: String, job: String, mgr: Long, sal: Double)
  case class Dept(deptno: Long, dname: String, loc: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL DatasetConvert example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // 隐式转化，包含了将RDD转化为DateFrame的操作
    import spark.implicits._

    /*
    // 外部数据集创建DataSet
    val ds = spark.read.json("src/main/resources/emp.json").as[Emp]

    // 内部数据创建DataSet
    val innerDs = Seq(Emp("ALLEN", 300.0, 30, 7499, "1981-02-20 00:00:00", "SALESMAN", 7698, 1600.0),
                      Emp("JONES", 300.0, 30, 7499, "1981-02-20 00:00:00", "SALESMAN", 7698, 1600.0))
                  .toDS()
    ds.show()
    innerDs.show()
     */

    // StructField指定列创建dataframe
    val fields = Array(StructField("deptno", LongType, nullable = true),
                       StructField("dname", StringType, nullable = true),
                       StructField("loc", StringType, nullable = true))

    val schema = StructType(fields)
    val deptRDD = spark.sparkContext.textFile("src/main/resources/dept.txt")
    val rowRDD = deptRDD.map(_.split("\t")).map(line => Row(line(0).toLong, line(1), line(2)))
    val deptDF = spark.createDataFrame(rowRDD, schema)
    deptDF.show()

    // dataframe 转 dataSets
    // df.as[Emp]
    // org.apache.spark.sql.Dataset[Emp] = [COMM: double, DEPTNO: bigint ... 6 more fields]
    //
    //# Datasets转DataFrames
    // ds.toDF()
    // org.apache.spark.sql.DataFrame = [COMM: double, DEPTNO: bigint ... 6 more fields]


  }
}
