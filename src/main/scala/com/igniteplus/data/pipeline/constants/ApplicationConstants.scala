package com.igniteplus.data.pipeline.constants

import com.igniteplus.data.pipeline.cleanseData.CleanData
import com.igniteplus.data.pipeline.util.ApplicationUtil.sparkSessionCreate
import com.igniteplus.data.pipeline.service.FileReaderService.readFile


import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


object ApplicationConstants
{
  //SPARK SESSION
  val MASTER_NAME:String="local"
  val APP_NAME:String="DE PRODUCT"
  implicit val spark:SparkSession=sparkSessionCreate(MASTER_NAME,APP_NAME)

  //LOCATION AND FILE TYPE
  val INPUT_LOCATION_CLICKSTREAM:String="data/Input/clickstream/clickstream_log.csv"
  val INPUT_LOCATION_ITEM:String="data/Input/item/item_data.csv"
  val INPUT_NULL_CLICKSTREAM_DATA:String="data/Output/pipeline-failures/nullClickstreamData.csv"
  val INPUT_NULL_ITEM_DATA:String="data/Output/pipeline-failures/nullItemData.csv"
  val FILE_TYPE:String="csv"

  val NIL_VALUE:String = "nil"


  //Input Column Names
  val EVENT_TIMESTAMP:String = "event_timestamp"
  val REDIRECTION_SOURCE:String="redirection_source"
  val DEPARTMENT_NAME:String="department_name"


  /** CLICKSTREAM DATATYPE VALIDATION */
  val COL_TIMESTAMP:String = EVENT_TIMESTAMP
  val TO_TIMESTAMP:String = "timestamp"
  val TIMESTAMP_FORMAT:String = "MM/dd/yyyy H:mm"

  /** DE_DUPLICATION */
  val SEQ_CLICKSTREAM_PRIMARY_KEYS:Seq[String] = Seq("session_id","item_id")
  val SEQ_ITEM_PRIMARY_KEYS:Seq[String] = Seq("item_id")

  /** CHECKING NULL VALUES */
  val CLICKSTREAM_COLUMNS_CHECK_NULL : Seq[String] = Seq("event_timestamp","device_type","session_id","visitor_id","item_id","redirection_source")
  val ITEM_COLUMNS_CHECK_NULL : Seq[String] = Seq("item_id","item_price","product_type","department_name")

  //Output Column Names

  //File Formats
}