package com.igniteplus.data.pipeline.cleanseData

import org.apache.spark.sql.functions.{col, length, ltrim, trim}
import org.apache.spark.sql.{Column, DataFrame, functions}

object spaceRemoval
{
  def removeSpaces(df:DataFrame,colName:Column):DataFrame={
    val trimmedDf:DataFrame=df.withColumn("length_without_trimming",length(col(colName.toString())))
      .withColumn(colName.toString(),trim(col(colName.toString())))
      .withColumn("length_with_triming",functions.length(col(colName.toString())))
    trimmedDf
  }

}
