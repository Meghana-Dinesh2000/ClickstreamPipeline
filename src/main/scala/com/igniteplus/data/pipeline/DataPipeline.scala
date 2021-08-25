
package com.igniteplus.data.pipeline


import com.igniteplus.data.pipeline.constants.ApplicationConstants.{FAILURE_EXIT_CODE, spark}
import com.igniteplus.data.pipeline.exception.{DqDuplicateCheckException, DqNullCheckException, FileReaderException, FileWriterException}
import com.igniteplus.data.pipeline.service.DqCheckService.executeDqCheck
import com.igniteplus.data.pipeline.service.PipelineService.pipelineService
import org.apache.spark.internal.Logging
import com.sun.org.slf4j.internal
import com.sun.org.slf4j.internal.LoggerFactory


object DataPipeline extends Logging
{
  def main(args: Array[String]): Unit =
  {
    val t1 = System.nanoTime()
    val logger :internal.Logger = LoggerFactory.getLogger(this.getClass)
    try {
      pipelineService()
      executeDqCheck()
    }
    catch
      {
        case ex : FileReaderException =>
          logError("File Reader Exception",ex)
          sys.exit(FAILURE_EXIT_CODE)
        case ex : FileWriterException =>
          logError("File Writer Exception",ex)
          sys.exit(FAILURE_EXIT_CODE)
        case ex : DqDuplicateCheckException =>
          logError("Dq check failed",ex)
          sys.exit(FAILURE_EXIT_CODE)
        case ex : DqNullCheckException =>
          logError("Dq check failed",ex)
          sys.exit(FAILURE_EXIT_CODE)
        case ex:Exception =>
          logError("Unkonwn Exception",ex)
          sys.exit(FAILURE_EXIT_CODE)
      }

    finally
    {
      val duration = (System.nanoTime()-t1)/1e9d
      println(duration)
      println("Finally exiting...")
      spark.stop()
    }
  }
}