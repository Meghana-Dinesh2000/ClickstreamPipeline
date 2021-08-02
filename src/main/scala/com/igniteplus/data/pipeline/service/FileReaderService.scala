package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.{DataFrame, SparkSession}
object FileReaderService
{
  /**Reading the  data*/
   def readFile(path : String, inputType : String)(implicit spark : SparkSession) : DataFrame =
   {
      val fileDf : DataFrame = spark.read
        .option("header","true")
        .option("inferSchema","true")
        .format(inputType).load(path)
     fileDf
  }
}
