package com.igniteplus.data.pipeline.Service

import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.exception.FileReaderException
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class FileReaderServiceTest extends AnyFlatSpec with Helper {

  "readFile() method" should "read data from the given location" in {
    val readFileTestDf: DataFrame = readFile(READ_LOCATION, FILE_FORMAT)
    val readFileTestDfCount: Long = readFileTestDf.count()
    assertResult(COUNT_SHOULD_BE)(readFileTestDfCount)
  }

  "readFile() method" should "throw exception in case it's not able to read data" in {
    assertThrows[FileReaderException] {
      val readingFromTheWrongLocationDf: DataFrame = readFile(READ_WRONG_LOCATION, FILE_FORMAT)
    }
  }
}