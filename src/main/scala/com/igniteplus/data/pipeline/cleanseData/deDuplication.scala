package com.igniteplus.data.pipeline.cleanseData


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}

object deDuplication
{
  //Method-1
  def deDuplication(df:DataFrame):DataFrame= {
    val winSpec = Window.partitionBy("session_id", "item_id").orderBy(desc("event_timestamp"))
    val duplicate: DataFrame = df.withColumn("row_number", row_number().over(winSpec))
    val deDuplicate: DataFrame = duplicate.filter("row_number==1").drop("row_number")
    deDuplicate
  }
  //METHOD 2
//  def dropDuplicates(Df:DataFrame)(implicit spark:SparkSession): DataFrame = {
//    val clickStreamData: DataFrame = readFile(INPUT_LOCATION_CLICKSTREAM)(spark)
//    val dropped = clickStreamData.dropDuplicates("device_type", "session_id", "visitor_id", "item_id")
//    dropped
//  }

}
