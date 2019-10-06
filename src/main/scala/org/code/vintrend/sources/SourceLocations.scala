package org.code.vintrend.sources

import org.apache.spark.internal.Logging

/**
  * Created by "venkatbhimi" on 04/10/2019.
  */
trait SourceLocations extends Logging with Serializable {

  def properties :Map[String, String]

    lazy  val pathtitleratings 	: String = getProperty("pathtitleratings")
    lazy  val pathcrew 	        : String = getProperty("pathcrew")
    lazy  val pathnames 	      : String = getProperty("pathnames")
    lazy  val titleNames 	      : String = getProperty("titleNames")

    def getProperty(param:String): String = {
      try{
        properties.get(param).get.trim
      } catch {
        case e: java.util.NoSuchElementException => {
          logInfo(s"Parameter not exisits in the properties file => ${param} ")
          ""
        }
      }
  }
}
