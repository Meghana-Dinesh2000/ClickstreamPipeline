package com.igniteplus.data.pipeline.service


import com.igniteplus.data.pipeline.cleanseData.CleanData.{checkForNull, deDuplication, filterRemoveNull, removeSpaces}
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{CLICKSTREAM_COLUMNS_CHECK_NULL, COL_TIMESTAMP, CSV_FILE_TYPE, DEPARTMENT_NAME, EVENT_TIMESTAMP, INPUT_LOCATION_CLICKSTREAM, INPUT_LOCATION_ITEM, INPUT_NULL_CLICKSTREAM_DATA, INPUT_NULL_ITEM_DATA, ITEM_COLUMNS_CHECK_NULL, NIL_VALUE, REDIRECTION_SOURCE, SEQ_CLICKSTREAM_PRIMARY_KEYS, SEQ_ITEM_PRIMARY_KEYS, TIMESTAMP_FORMAT, TO_TIMESTAMP, spark}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.transformation.Transform.{consistentNaming, dataTypeValidation}
import org.apache.spark.sql.DataFrame


object PipelineService
{
  def pipelineService () : Unit  =
  {
    /** Reading Clickstream Data */
    val clickstreamDf : DataFrame = readFile(INPUT_LOCATION_CLICKSTREAM,CSV_FILE_TYPE)

    /** Reading Item Data */
    val itemDf : DataFrame = readFile(INPUT_LOCATION_ITEM,CSV_FILE_TYPE)

    /**Changing to appropriate datatype*/
    val validatedClickstremDf : DataFrame = dataTypeValidation(clickstreamDf,COL_TIMESTAMP,TO_TIMESTAMP,TIMESTAMP_FORMAT)

    /**Checking for null vlaues and filtering them*/
    val notNullClickstreamDf : DataFrame = filterRemoveNull(validatedClickstremDf,SEQ_CLICKSTREAM_PRIMARY_KEYS,INPUT_NULL_CLICKSTREAM_DATA,CSV_FILE_TYPE)
    val notNullItemDf : DataFrame = filterRemoveNull(itemDf,SEQ_ITEM_PRIMARY_KEYS,INPUT_NULL_ITEM_DATA,CSV_FILE_TYPE)

//    println("Number of clickstream data rows before deduplication="+notNullClickstreamDf.count())
//    println("Number of item data rows before deduplication="+notNullItemDf.count())
    /**Removing duplicates from data*/
    val deduplicatedClickstreamDf : DataFrame = deDuplication(notNullClickstreamDf,SEQ_CLICKSTREAM_PRIMARY_KEYS)
    val deduplicatedItemDf : DataFrame = deDuplication(notNullItemDf,SEQ_ITEM_PRIMARY_KEYS)

//    println("Number of clickstream data rows after deduplication="+deduplicatedClickstreamDf.count())
//    println("Number of item data rows after deduplication="+deduplicatedItemDf.count())
    /**Changing the names to appropriate form by naming them consistently*/
    val consistentNameClickstreamDf : DataFrame = consistentNaming(deduplicatedClickstreamDf,REDIRECTION_SOURCE)
    val consistentItemDf : DataFrame = consistentNaming(deduplicatedItemDf,DEPARTMENT_NAME)

    /**Trimming the spaces present in column values*/
    val trimmedClickstreamDf : DataFrame = removeSpaces(consistentNameClickstreamDf,REDIRECTION_SOURCE)
    val trimmedItemDf : DataFrame = removeSpaces(consistentItemDf,DEPARTMENT_NAME)
  }
}
