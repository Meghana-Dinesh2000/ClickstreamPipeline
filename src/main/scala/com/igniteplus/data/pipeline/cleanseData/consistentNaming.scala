package com.igniteplus.data.pipeline.cleanseData

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, expr, initcap}

object consistentNaming
{
  def consistentNaming(df:DataFrame,nameCol:Column): DataFrame =
  {
    val consistentNames:DataFrame=df.withColumn(nameCol.toString(),initcap(col(nameCol.toString())))
    consistentNames
  }
}
