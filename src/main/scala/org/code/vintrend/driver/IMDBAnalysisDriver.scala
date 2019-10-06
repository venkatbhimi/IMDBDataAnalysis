package org.code.vintrend.driver

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.code.vintrend.pipelines.IMDBAnalysisPipeline
import org.code.vintrend.sinks.CSVSink

/**
  * Created by "venkatbhimi" on 04/10/2019.
  */
object IMDBAnalysisDriver extends BuildSparkContext with Logging {

  /**
    *
    * @param args
    */
  def main(args: Array[String]) {
    lazy val spark          : SparkSession  = SparkSession.builder.master("local").appName("IMDB DataSet Analysis").getOrCreate()

    initContext(spark) // adding spark config parameters to conf.
    val propertiesFileLocation  : String  = args(0)
    val resourceManager         : String  = args(1)
    val baseDir                 : String  = args(2)
    val titleType               : String  = args(3)
    val topXRanks               : String  = args(4)
    val outputPath              : String  = args(5)


    lazy val properties: Map[String,String] = loadProperties(propertiesFileLocation, spark, baseDir)

    logInfo(s"**********************INPUT-PARAMETERS*****************************")
    logInfo(s"Properties Location       => ${propertiesFileLocation}")
    logInfo(s"Resource Mananger  => ${resourceManager}")
    logInfo(s"*******************************************************************")

    try {
        val imdbAnalysisPipeline = IMDBAnalysisPipeline(spark, properties)
        val titilesTopXranks     = imdbAnalysisPipeline.runPipeline(titleType = "movies", topXRanks = 20)
        val outputFileName       = titleType+"_"+topXRanks+".csv"
        CSVSink.sink(outputPath+outputFileName, titilesTopXranks)
      }
    catch {
        case e:Exception => {
        logError(s"Exception in processing IMDB Analysis Pipeline data ${e.printStackTrace}")
        throw e
        }
    } finally {
    spark.stop()
    }
  }

}
