package org.code.vintrend.sinks

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  *
  * writing results into a csv-file
  *
  * Created by "venkatbhimi" on 05/10/2019.
  */
object CSVSink {

  def sink(outputPath:String, finalDataSet:DataFrame) = {
    finalDataSet.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode(SaveMode.Overwrite).save(outputPath)
  }

}
