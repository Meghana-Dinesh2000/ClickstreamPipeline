package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileReaderException
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
object FileReaderService
{

  /**Reading the  data*/
    @throws(classOf[FileReaderException])
     def readFile(filePath : String, fileType : String)(implicit spark : SparkSession): DataFrame  = {

     val fileDf: DataFrame = try {
       spark.read
         .option("header", "true")
         .option("delimiter",",")
         .format(fileType)
         .load(filePath)

     }
     catch {
       case e: Exception =>
         throw FileReaderException("Exception in file reading")
     }

    val fileDfCount: Long = fileDf.count()
     if(fileDfCount == 0)
       throw FileReaderException("No files read from the location "+ s"$filePath")
     fileDf
  }
}
