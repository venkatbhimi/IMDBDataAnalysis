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

  if(args.length < 6) {
      logInfo(s"**********************start-applicaiton*****************************")
      logInfo(" Expected arguments 6, Please check the passing arguments, currently passing only: "+args.length)
      logInfo(" Arguments should contain, PropertiesLocation: String , RM:String, baseDir:String, titleType:String (ex:movie),topXRanks:Int, outputPath:String")
      logInfo(s"**********************Exiting-applicaiton*****************************")
      System.exit(0)
    } else {
      logInfo(s"**********************INPUT-PARAMETERS*****************************")
      logInfo(s"Properties Location       => ${args(0)}")
      logInfo(s"Resource Mananger         => ${args(1)}")
      logInfo(s"baseDir                   => ${args(2)}")
      logInfo(s"titleType                 => ${args(3)}")
      logInfo(s"topXRanks                 => ${args(4)}")
      logInfo(s"outputPath                => ${args(5)}")
      logInfo(s"*******************************************************************")
    }

    lazy val spark          : SparkSession  = SparkSession.builder.master("local").appName("IMDB DataSet Analysis").getOrCreate()

    initContext(spark) // adding spark config parameters to conf.

    val propertiesFileLocation  : String  = args(0)
    val resourceManager         : String  = if(args(1) == "NA") "" else args(1)
    val baseDir                 : String  = resourceManager + args(2)
    val titleType               : String  = args(3)
    val topXRanks               : Int     = args(4).toInt
    val outputPath              : String  = args(5)

    lazy val properties: Map[String,String] = loadProperties(propertiesFileLocation, spark, baseDir.trim)

    try {
        val imdbAnalysisPipeline = IMDBAnalysisPipeline(spark, properties)
        val titilesTopXranks     = imdbAnalysisPipeline.runPipeline(titleType, topXRanks)
        val outputFileName       = titleType+"_"+topXRanks+".csv"
        //titilesTopXranks.show(20)
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
