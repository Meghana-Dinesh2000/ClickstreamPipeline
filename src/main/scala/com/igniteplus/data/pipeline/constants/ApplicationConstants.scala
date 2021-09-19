package com.igniteplus.data.pipeline.constants

import com.igniteplus.data.pipeline.util.ApplicationUtil.{createSparkSession, getSparkConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ApplicationConstants
{
  /**Variables for Spark Session*/
  val SPARK_CONF_FILE_NAME : String = "spark.conf"
  val SPARK_CONF : SparkConf = getSparkConf(SPARK_CONF_FILE_NAME)
  implicit val spark : SparkSession = createSparkSession(SPARK_CONF)

  /**Location of various files*/
  val INPUT_LOCATION_CLICKSTREAM : String = "data/Input/clickstream/clickstream_log.csv"
  val INPUT_LOCATION_ITEM : String = "data/Input/item/item_data.csv"
  val INPUT_NULL_CLICKSTREAM_DATA : String = "data/Output/Pipeline-failures/NullClickstreamData"
  val INPUT_NULL_ITEM_DATA : String = "data/Output/Pipeline-failures/NullItemData"

  /**Various file format types*/
  val CSV_FILE_TYPE : String = "csv"

  /**Nil value*/
  val NIL_VALUE : String = "nil"

  /**Exit code*/
  val FAILURE_EXIT_CODE : Int = 1
  val SUCCESS_EXIT_CODE : Int = 0

  /**Input Column Names*/
  val EVENT_TIMESTAMP : String = "event_timestamp"
  val REDIRECTION_SOURCE : String = "redirection_source"
  val DEPARTMENT_NAME : String = "department_name"
  val SESSION_ID : String = "session_id"
  val ITEM_ID : String = "item_id"
  val VISITOR_ID : String = "visitor_id"

  /** Clickstream Datatype Validation */
  val COL_TIMESTAMP : String = EVENT_TIMESTAMP
  val   TO_TIMESTAMP : String = "timestamp"
  val TIMESTAMP_FORMAT : String = "MM/dd/yyyy H:mm"

  /** Sequence of primary keys used for deduplication */
  val SEQ_CLICKSTREAM_PRIMARY_KEYS : Seq[String] = Seq("session_id", "item_id")
  val SEQ_ITEM_PRIMARY_KEYS : Seq[String] = Seq("item_id")

  /**Sequence of columns checked for null values*/
  val CLICKSTREAM_COLUMNS_CHECK_NULL : Seq[String] = Seq("event_timestamp", "device_type", "session_id", "visitor_id", "item_id", "redirection_source")
  val ITEM_COLUMNS_CHECK_NULL : Seq[String] = Seq("item_id", "item_price", "product_type", "department_name")

  /** Write to SQL Database */
  val JDBC_DRIVER : String = "com.mysql.cj.jdbc.Driver"
  val USER_NAME : String = "root"
  val SQL_URL : String = "jdbc:mysql://localhost:3306/ignite"
  val KEY_PASSWORD : String = "meghana"
  val LOCATION_SQL_PASSWORD : String = "E:\\targetDEProduct_SQLPassword.txt"
  val LOCATION_ENCRYPTED_PASSWORD : String = "credentials/SQL_password_file"
  val TABLE_CLICKSTREAM_DATA : String = "CLICKSTREAM_DATA"
  val TABLE_ITEM_DATA : String = "ITEM_DATA"
  val KEY_TYPE : String = "JCEKS"
  val KEY_LOCATION : String = "credentials/mykeystore.jks"
  val CRYPTOGRAPHY_ALGORITHM : String = "AES"
  val KEY_ALIAS : String = "mykey"

  /** Join CONDITION */
  val JOIN_KEY : String = "item_id"
  val JOIN_TYPE : String = "leftouter"

  /** DQ Checks */
  val ROW_NUMBER : String = "row_number"
  val ROW_CONDITION : String = "row_number==1"


  val SQL_URL_STAGING : String = "jdbc:mysql://localhost:3306/ignite_staging"
  val SQL_URL_PROD : String = "jdbc:mysql://localhost:3306/ignite_prod"
  val TABLE_NAME :String ="final_data"
  val COLUMNS_CHECK_NULL_DQ_CHECK: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ITEM_ID,ApplicationConstants.VISITOR_ID)
}