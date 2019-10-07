package org.code.vintrend.pipeline


import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.code.vintrend.driver.BuildSparkContext
import org.code.vintrend.driver.IMDBAnalysisDriver._
import org.code.vintrend.pipelines.IMDBAnalysisPipeline
import org.code.vintrend.sinks.CSVSink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Integration Test - to run the pipeline.
  *
  * Created by "venkatbhimi" on 06/10/2019.
  */
class IMDBAnalysisPipelineTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll with Logging with BuildSparkContext {

  val propertiespath = "./src/test/resources/test-imdb-dataset.properties"
  val baseDir = "./src/test/resources"
  var spark         :SparkSession   = _
  var properties: Map[String,String] = null
  val outputdir = baseDir+"/output/"
  val titleType = "movie"
  val topXRanks = "20".toInt


  override def beforeEach() {
    spark     = SparkSession.builder.master("local").appName("IMDBTests")
      .config("spark.driver.maxResultSize", 0)
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    initContext(spark)
     properties = loadProperties(propertiespath, spark, baseDir)
  }

  test( "test pipeline" ) {
    val imdbAnalysisPipeline = IMDBAnalysisPipeline(spark, properties)
    val titilesTopXranks     = imdbAnalysisPipeline.runPipeline(titleType, topXRanks)
    val outputFileName       = titleType+"_"+topXRanks+".csv"
    CSVSink.sink(outputdir+"/"+outputFileName, titilesTopXranks)
  }

  override def afterEach() {
    spark.stop()
  }

}
