package org.code.vintrend.sources

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSuite, WordSpec}

/**
  * Created by "venkatbhimi" on 05/10/2019.
  */
class TSVFileReaderTest extends FunSuite with BeforeAndAfterEach {

  var spark         :SparkSession   = _
  var tSVFileReader : TSVFileReader= null

  override def beforeEach() {
    spark     = SparkSession.builder.master("local").appName("IMDBTests")
      .config("spark.driver.maxResultSize", 0)
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    tSVFileReader =  TSVFileReader(spark)
  }

  test("test for readTitleNames ")   {

    val schema = StructType(Array(StructField("value", StringType)))
    val rowRDD = spark.sparkContext.parallelize(Array(Row("tt0000001\tshort\tCarmencita\tCarmencita\t0\t1894\t\\N\t1\tDocumentary")))
    val df1 = spark.createDataFrame(rowRDD, schema)

    val df_titlesnames = tSVFileReader.readTitleNames(df1).rdd.take(1)
    assert(df_titlesnames(0)(0) === "tt0000001")
    assert(df_titlesnames(0)(1) === "short")
    assert(df_titlesnames(0)(2) === "Carmencita")
    assert(df_titlesnames(0)(3) === "Carmencita")
    assert(df_titlesnames(0)(4) === "0")
    assert(df_titlesnames(0)(5) === "1894")
    assert(df_titlesnames(0)(6) === "\\N")
    assert(df_titlesnames(0)(7) === "1")
    assert(df_titlesnames(0)(8).toString === "WrappedArray(Documentary)")
  }

  test("test for readTitleCrewDataSet ")   {

    val schema = StructType(Array(StructField("value", StringType)))
    val rowRDD = spark.sparkContext.parallelize(Array(Row("tt0000001\tnm0005690\t\\N")))
    val df1 = spark.createDataFrame(rowRDD, schema)
    val dfWithNames = tSVFileReader.readingCrewNames(df1).rdd.take(1)
    assert(dfWithNames(0)(0) === "tt0000001")
    assert(dfWithNames(0)(1) === "nm0005690")
    assert(dfWithNames(0)(2) === "\\N")
    assert(dfWithNames(0)(3) === "")
    assert(dfWithNames(0)(4) === Array(""))
    assert(dfWithNames(0)(5) === Array(""))
  }

  test("test for readTitleRatings ")   {

    val schema = StructType(Array(StructField("value", StringType)))
    val rowRDD = spark.sparkContext.parallelize(Array(Row("tt0000001\t5.6\t1539")))
    val df1 = spark.createDataFrame(rowRDD, schema)
    val dfWithNames = tSVFileReader.readTitleRatings(df1).rdd.take(1)
    assert(dfWithNames(0)(0) === "tt0000001")
    assert(dfWithNames(0)(1) === 5.6)
    assert(dfWithNames(0)(2) === 1539)
  }

  test("test for readingCrewNames ")   {

    val schema = StructType(Array(StructField("value", StringType)))
    val rowRDD = spark.sparkContext.parallelize(Array(Row("tt0000001\tnm0005690\t\\N")))
    val df1 = spark.createDataFrame(rowRDD, schema)
    val df_withCrew = tSVFileReader.readTitleCrewDataSet(df1).rdd.take(1)
    assert(df_withCrew(0)(0) === "tt0000001")
    assert(df_withCrew(0)(1) === "nm0005690")
    assert(df_withCrew(0)(2) === "\\N")
  }

  override def afterEach() {
    spark.stop()
  }
}
