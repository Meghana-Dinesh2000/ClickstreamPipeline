package com.igniteplus.data.pipeline.cleanseData

import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.cleanseData.CleanData.{dataTypeValidation, deDuplication}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class CleanseTest extends AnyFlatSpec with Helper{

  "deDuplication() method" should "remove the duplicates from the inputDF" in {
    import spark.implicits._
    val deDuplicatedFileTestDf : DataFrame = Seq(
      ("29839","11/15/2020 15:11","android","B000078","I7099","B17543","GOOGLE"),
      ("30504","11/15/2020 15:27","android","B000078","I7099","B17543","LinkedIn"),
      ("30334","11/15/2020 15:23","android","B000078","I7099","B17543","Youtube"),
      ("30385","11/15/2020 15:24","android","B000078","I7099","D8142","google")
    ).toDF("id","event_timestamp","device_type","session_id","visitor_id","item_id","redirection_source")
    val deDuplicatedDF : DataFrame = deDuplication(deDuplicatedFileTestDf,PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA,Some(ORDER_BY_COLUMN))
    val expectedDF : DataFrame = Seq(
      ("30504","11/15/2020 15:27","android","B000078","I7099","B17543","LinkedIn"),
      ("30385","11/15/2020 15:24","android","B000078","I7099","D8142","google")
    ).toDF("id","event_timestamp","device_type","session_id","visitor_id","item_id","redirection_source")
    val resultantDF : DataFrame = expectedDF.except(deDuplicatedDF)
    val output : Long = resultantDF.count()
    val expectedCount : Long = 0
    assertResult(expectedCount)(output)
  }

  "Function  changeDataType" should "Check the data type in the dataframe " in {
    val sampleDF: DataFrame = readFile(CHANGE_DATATYPE_TEST_READ, fileFormat)
    val changeDataTypeDF: DataFrame = dataTypeValidation(sampleDF, COLUMNS_VALID_DATATYPE_CLICKSTREAM, NEW_DATATYPE_CLICKSTREAM,TIMESTAMP_FORMAT)
    //val result: Boolean = (sampleDF.schema("event_timestamp").dataType === changeDataTypeDF.schema("event_timestamp").dataType)
    val result: Boolean = (changeDataTypeDF.schema("event_timestamp").dataType.typeName === "timestamp")
    assertResult(expected = true)(result)
  }

}
