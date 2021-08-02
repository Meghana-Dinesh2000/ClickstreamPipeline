package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.DataFrame

object FileWriterService
{
  def writeFile(df:DataFrame, fileType:String, filePath:String) : Unit =
  {
    df.write.format(fileType)
      .option("header","true")
      .mode("overwrite")
      .option("sep",",")
      .save(filePath)
  }

}
