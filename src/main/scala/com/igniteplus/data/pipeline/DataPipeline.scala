package com.igniteplus.data.pipeline


import com.igniteplus.data.pipeline.exception.{FileReaderException, FileWriterException}
import com.igniteplus.data.pipeline.service.PipelineService.pipelineService
import org.apache.log4j.spi.LoggerFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal

object DataPipeline extends Logging
{
  def main(args: Array[String]): Unit =
  {
    val t1 = System.nanoTime()

    try {
      pipelineService()
    }
    catch
      {
        case ex : FileReaderException => println(ex)
        case ex: FileWriterException => println(ex)
      }
    val duration = (System.nanoTime()-t1)/1000000000
    println(duration)
  }
}