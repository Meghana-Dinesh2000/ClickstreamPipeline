package com.igniteplus.data.pipeline.cleanseData

import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.cleanseData.CleanData.{dataTypeValidation, deDuplication}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class CleanseTest extends AnyFlatSpec with Helper{

  "removeDuplicates() method" should "remove the duplicates from the inputDF" in {
    val deDuplicatedFileTestDf : DataFrame = readFile(DEDUPLICATION_TEST_READ, FILE_FORMAT)(spark)
    val deDuplicatedDF : DataFrame = deDuplication(deDuplicatedFileTestDf,PRIMARY_KEY_COLUMNS_CLICKSTREAM_DATA,Some(ORDER_BY_COLUMN))
    val deDuplicatedCount : Long = deDuplicatedDF.count()
    val expectedCount : Long = 2
    assertResult(expectedCount)(deDuplicatedCount)
  }

  "Function  changeDataType" should "Check the data type in the dataframe " in {
    val sampleDF: DataFrame = readFile(CHANGE_DATATYPE_TEST_READ, fileFormat)
    val changeDataTypeDF: DataFrame = dataTypeValidation(sampleDF, COLUMNS_VALID_DATATYPE_CLICKSTREAM, NEW_DATATYPE_CLICKSTREAM,"csv")
    //val result: Boolean = (sampleDF.schema("event_timestamp").dataType === changeDataTypeDF.schema("event_timestamp").dataType)
    val result: Boolean = (changeDataTypeDF.schema("event_timestamp").dataType.typeName === "timestamp")
    assertResult(expected = true)(result)
  }

}
