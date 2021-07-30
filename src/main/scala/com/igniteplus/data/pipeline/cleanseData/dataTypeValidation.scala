package com.igniteplus.data.pipeline.cleanseData


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_timestamp, unix_timestamp}

object dataTypeValidation
{
  def dataTypeValidation(df:DataFrame):DataFrame =
  {
    val dataValidated=df
      .withColumn("event_timestamp",unix_timestamp(col("event_timestamp"),"MM/dd/yyyy H:mm")
        .cast("double")
        .cast("timestamp"))
    dataValidated
  }
}


