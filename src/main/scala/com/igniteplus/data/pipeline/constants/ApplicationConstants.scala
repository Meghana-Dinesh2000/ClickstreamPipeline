package com.igniteplus.data.pipeline.constants

import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.sql.SparkSession

object ApplicationConstants
{
  /**Variables for Spark Session*/
  val MASTER_NAME : String = "local"
  val APP_NAME : String = "DE PRODUCT"
  implicit val spark : SparkSession = createSparkSession(MASTER_NAME, APP_NAME)

  /**Location of various files*/
  val INPUT_LOCATION_CLICKSTREAM : String = "data/Input/clickstream/clickstream_log.csv"
  val INPUT_LOCATION_ITEM : String = "data/Input/item/item_data.csv"
  val INPUT_NULL_CLICKSTREAM_DATA : String = "data/Output/Pipeline-failures/NullClickstreamData"
  val INPUT_NULL_ITEM_DATA : String = "data/Output/Pipeline-failures/NullItemData"

  /**Various file format types*/
  val CSV_FILE_TYPE : String = "csv"

  /**Nil value*/
  val NIL_VALUE : String = "nil"


  /**Input Column Names*/
  val EVENT_TIMESTAMP : String = "event_timestamp"
  val REDIRECTION_SOURCE : String = "redirection_source"
  val DEPARTMENT_NAME : String = "department_name"
  val SESSION_ID : String = "session_id"
  val ITEM_ID : String = "item_id"

  /** Clickstream Datatype Validation */
  val COL_TIMESTAMP : String = EVENT_TIMESTAMP
  val TO_TIMESTAMP : String = "timestamp"
  val TIMESTAMP_FORMAT : String = "MM/dd/yyyy H:mm"

  /** Sequence of primary keys used for deduplication */
  val SEQ_CLICKSTREAM_PRIMARY_KEYS : Seq[String] = Seq("session_id", "item_id")
  val SEQ_ITEM_PRIMARY_KEYS : Seq[String] = Seq("item_id")

  /**Sequence of columns checked for null values*/
  val CLICKSTREAM_COLUMNS_CHECK_NULL : Seq[String] = Seq("event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")
  val ITEM_COLUMNS_CHECK_NULL : Seq[String] = Seq("item_id", "item_price", "product_type", "department_name")
}