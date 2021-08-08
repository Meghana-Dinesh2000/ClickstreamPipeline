package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriterException
import org.apache.spark.sql.DataFrame

object FileWriterService
{
  def writeFile(df:DataFrame, fileType:String, filePath:String) : Unit =
  {
    try {
      df.write.format(fileType)
        .option("header", "true")
        .mode("overwrite")
        .option("sep", ",")
        .save(filePath)
    }
    catch{
      case e: Exception => FileWriterException("Unable to write files to the location "+ s"$filePath")
    }
  }

}
