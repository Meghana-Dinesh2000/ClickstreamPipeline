package com.igniteplus.data.pipeline


import com.igniteplus.data.pipeline.constants.ApplicationConstants.FAILURE_EXIT_CODE
import com.igniteplus.data.pipeline.exception.{FileReaderException, FileWriterException}
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
          sys.exit(FAILURE_EXIT_CODE)
        case ex: FileWriterException =>
          logError("File Writer Exception",ex)
          sys.exit(FAILURE_EXIT_CODE)
        case ex:Exception =>
          logError("Unkonwn Exception",ex)
          sys.exit(FAILURE_EXIT_CODE)
      }
    val duration = (System.nanoTime()-t1)/1e9d
    println(duration)
  }
}
