package com.igniteplus.data.pipeline.Helper

import com.igniteplus.data.pipeline.constants.ApplicationConstants.SPARK_CONF
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession

trait Helper {

  implicit val spark = createSparkSession(SPARK_CONF)


  /* Helpers for File Reader Service Test Case */
  val READ_LOCATION : String = "data/Test_Inputs/FileReaderServiceTestCaseInput.csv"
  val FILE_FORMAT : String = "csv"
  val COUNT_SHOULD_BE : Int = 4
  val READ_WRONG_LOCATION : String = "data/Test_Inputs/FileReaderServiceTestCaseInp.csv"

  /* Helpers for File Writer Service Test Case */
  val writeTestCaseInputPath ="data/Test_Inputs/FileWriterServiceTestCaseInput.csv"
  val fileFormat = "csv"
  val writeTestCaseOutputPath = "data/Test_Outputs/FileWriterServiceTestCaseOutput.csv"

  /* Helpers for removeDuplicates Test Case*/
  val DEDUPLICATION_TEST_READ : String = "data/Test_Inputs/DeDuplicationTestCaseInput.csv"
  val PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA : Seq[String] = Seq("session_id","item_id")
  val ORDER_BY_COLUMN : String = "event_timestamp"

  /* Helpers for join Test Case*/
  val JOIN_KEY : String = "item_id"
  val JOIN_TYPE : String = "left"

  /* Helpers for Change Data type test case */
  val CHANGE_DATATYPE_TEST_READ : String = "data/Test_Inputs/DatatypeValidationTestCaseInputRead.csv"
  val EVENT_TIMESTAMP:String = "timestamp"
  val COLUMNS_VALID_DATATYPE_CLICKSTREAM : String = "event_timestamp"
  val NEW_DATATYPE_CLICKSTREAM : String = "timestamp"
  val TIMESTAMP_FORMAT : String = "MM/dd/yyyy H:mm"

  /** Helpers for DQ Checks Method */
  val PRIMARY_KEY_COLUMNS : Seq[String] = Seq("session_id","item_id")
  val ORDER_BY_COL : String = "event_timestamp"
}
