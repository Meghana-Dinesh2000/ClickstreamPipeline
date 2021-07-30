package com.igniteplus.data.pipeline.constants

import com.igniteplus.data.pipeline.util.ApplicationUtil.sparkSessionCreate
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.cleanseData.consistentNaming.consistentNaming
import com.igniteplus.data.pipeline.cleanseData.dataTypeValidation.dataTypeValidation
import com.igniteplus.data.pipeline.cleanseData.deDuplication.deDuplication
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ApplicationConstants
{
  //SPARK SESSION
  val MASTER_NAME:String="local"
  val APP_NAME:String="DE PRODUCT"
  val spark:SparkSession=sparkSessionCreate(MASTER_NAME,APP_NAME)

  //LOCATION AND FILE TYPE
  val INPUT_LOCATION_CLICKSTREAM:String="data/Input/clickstream/clickstream_log.csv"
  val INPUT_LOCATION_ITEM:String="data/Input/item/item_data.csv"
  val FILE_TYPE:String="csv"

  //DATAFRAMES
  val CLICKSTREAM_DATA:DataFrame=readFile(INPUT_LOCATION_CLICKSTREAM,FILE_TYPE)(spark)
  val ITEM_DATA:DataFrame=readFile(INPUT_LOCATION_ITEM,FILE_TYPE)(spark)
  val VALIDATED_DATA:DataFrame=dataTypeValidation(CLICKSTREAM_DATA)
  val DEDUPLICATED_DATA:DataFrame=deDuplication(VALIDATED_DATA)
  val CONSISTENT_NAMES:DataFrame=consistentNaming(DEDUPLICATED_DATA,DEDUPLICATED_DATA("redirection_source"))

  //Input Column Names
  val

  //Output Column Names

  //File Formats
}
