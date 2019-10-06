package org.code.vintrend.driver

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by "venkatbhimi" on 04/10/2019.
  */
trait BuildSparkContext {

  def getFileSystem(sc:SparkContext) = {
    FileSystem.get( sc.hadoopConfiguration)
  }
  /**
    * adding spark configurations to make sure job did not fail on default settings, forexample rpc timeout, network timeout.
    * @param spark
    * @return
    */
  def initContext(spark:SparkSession) = {

    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    spark.conf.set("spark.kryoserializer.buffer.max", "1g")
    spark.conf.set("spark.rdd.compress", "true")
    spark.conf.set("spark.network.timeout", "900")
    spark.conf.set("spark.rpc.askTimeout", "900")
    spark.conf.set("spark.broadcast.compress", "true")
    spark.conf.set("spark.akka.frameSize", "2047")
  }

  /**
    * loading properties file and splitting to key/value pairs and preparing a map.
    * @param fileName
    * @param spark
    * @return Map[String,String]
    */
  def loadProperties(fileName:String, spark:SparkSession, baseDirectory:String): Map[String,String] = {
    spark.sparkContext.textFile(fileName).map(x=> {
      val y = x.split("=>")
      (y(0), baseDirectory + y(1))
    }).collect.toMap
  }
}
