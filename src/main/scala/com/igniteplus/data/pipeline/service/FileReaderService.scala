package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileReaderException
import org.apache.spark.sql.{DataFrame, SparkSession}
object FileReaderService
{
  /**Reading the  data*/
   def readFile(filePath : String, fileType : String)(implicit spark : SparkSession) : DataFrame = {
     val fileDf: DataFrame = try {
       spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .format(fileType).load(filePath)
     }
     catch {
       case e: Exception => FileReaderException("Unable to read the files in given location "+ s"$filePath")
         spark.emptyDataFrame
     }
    val fileDfCount: Long=fileDf.count()
     if(fileDfCount == 0)
       throw FileReaderException("No files read from the location "+ s"$filePath")
     fileDf
  }
}
