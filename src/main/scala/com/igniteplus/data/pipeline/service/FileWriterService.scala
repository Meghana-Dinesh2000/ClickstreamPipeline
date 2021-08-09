package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriterException
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto.FileType
import org.apache.spark.sql.DataFrame

import java.io.FileWriter

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
  def generalWrite(content : String, filePath:String) : Unit=
    {
      try
        {
          val fw = new FileWriter(filePath,true)
          fw.write(content)
          fw.write("\n")
          fw.close()
        }
      catch
        {
          case e: Exception => FileWriterException("Unable to write wiles to the location" +s"$filePath")
        }
    }
}
