package com.igniteplus.data.pipeline


import com.igniteplus.data.pipeline.exception.{FileReaderException, FileWriterException}
import com.igniteplus.data.pipeline.service.FileWriterService.generalWrite
import com.igniteplus.data.pipeline.service.PipelineService.pipelineService
import org.apache.spark.internal.Logging
import com.sun.org.slf4j.internal
import com.sun.org.slf4j.internal.LoggerFactory
import org.apache.spark.internal._

object DataPipeline extends Logging
{
  def main(args: Array[String]): Unit =
  {
    val t1 = System.nanoTime()
    val logger :internal.Logger = LoggerFactory.getLogger(this.getClass)
    try {
      pipelineService()
    }
    catch
      {
        case ex : FileReaderException =>
          logError("File Reader Exception",ex)
          generalWrite(ex.toString,"data/Output/Pipeline-failures/FileReaderException")
        case ex: FileWriterException =>
          logError("File Writer Exception",ex)
        case ex:Exception =>
          logError("Unkonwn Exception",ex)
      }
    val duration = (System.nanoTime()-t1)/1e9d
    println(duration)
  }
}
