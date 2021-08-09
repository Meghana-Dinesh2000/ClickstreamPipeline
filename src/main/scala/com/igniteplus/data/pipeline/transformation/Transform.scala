package com.igniteplus.data.pipeline.transformation

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, initcap, unix_timestamp}

object Transform
{
  def dataTypeValidation(df : DataFrame, colName : String, to_datatype : String, format : String) : DataFrame =
  {
    if(format == "nil")
    {
        val dataValidated : DataFrame = df.withColumn(colName, df.col(colName).cast(to_datatype))
        dataValidated
    }
    else
    {
      val dataValidated : DataFrame = df
          .withColumn(colName, unix_timestamp(col(colName), format)
          .cast("double")
          .cast(to_datatype))
      dataValidated
    }
  }
  def consistentNaming(df : DataFrame, nameCol : String): DataFrame =
  {
    val consistentNames : DataFrame = df.withColumn(nameCol,initcap(col(nameCol)))
    //consistentNames.show()
    consistentNames
  }

}
