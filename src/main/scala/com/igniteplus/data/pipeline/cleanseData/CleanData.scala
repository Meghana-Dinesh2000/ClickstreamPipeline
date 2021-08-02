package com.igniteplus.data.pipeline.cleanseData

import com.igniteplus.data.pipeline.constants.ApplicationConstants.FILE_TYPE
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.apache.spark.sql.functions.{col, desc, initcap, row_number, trim, unix_timestamp}

object CleanData
{
  def removeNull(df:DataFrame, columnName:Seq[String], filePath:String, fileFormat:String) : DataFrame =
  {
    var nullDf : DataFrame = df
    var notNullDf : DataFrame = df
    for( i <- columnName)
    {
      nullDf = df.filter(df(i).isNull)
      notNullDf = df.filter(df(i).isNotNull)
    }
    writeFile(nullDf, fileFormat, filePath)
    notNullDf
  }
  def deDuplication(df : DataFrame, orderBy : String, colNames : String*) : DataFrame =
  {
    if(orderBy == "nil")
    {
      val deDuplicate : DataFrame = df.dropDuplicates(colNames.head, colNames.tail:_*)
      deDuplicate
    }
    else
    {
        val winSpec = Window.partitionBy(colNames.head, colNames.tail: _*)
                            .orderBy(desc(orderBy))
        val deDuplicate : DataFrame = df.withColumn("row_number", row_number().over(winSpec))
                                        .filter("row_number==1")
                                        .drop("row_number")
        deDuplicate
    }
  }

  def removeSpaces(df:DataFrame, colName:String) : DataFrame =
  {
    val trimmedDf: DataFrame = df
      // .withColumn("length_without_trimming", functions.length(col(colName)))
      .withColumn(colName, trim(col(colName)))
    // .withColumn("length_with_triming", functions.length(col(colName)))
    //    trimmedDf.show()
    trimmedDf
  }
}







