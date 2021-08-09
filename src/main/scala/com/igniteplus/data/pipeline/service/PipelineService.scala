package com.igniteplus.data.pipeline.service


import com.igniteplus.data.pipeline.cleanseData.CleanData.{ deDuplication, removeNull, removeSpaces}
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{CLICKSTREAM_COLUMNS_CHECK_NULL, COL_TIMESTAMP, DEPARTMENT_NAME, FILE_TYPE, INPUT_LOCATION_CLICKSTREAM, INPUT_LOCATION_ITEM, INPUT_NULL_CLICKSTREAM_DATA, INPUT_NULL_ITEM_DATA, ITEM_COLUMNS_CHECK_NULL, NIL_VALUE, REDIRECTION_SOURCE, SEQ_CLICKSTREAM_PRIMARY_KEYS, SEQ_ITEM_PRIMARY_KEYS, TIMESTAMP_FORMAT, TO_TIMESTAMP, spark}
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.transformation.Transform.{consistentNaming, dataTypeValidation}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, exp, expr}

object PipelineService
{
  def pipelineService ()  =
  {

    /** Reading Clickstream Data */
    val clickstreamDf:DataFrame=readFile(INPUT_LOCATION_CLICKSTREAM,FILE_TYPE)

    /** Reading Item Data */
    val itemDf:DataFrame=readFile(INPUT_LOCATION_ITEM,FILE_TYPE)


    val validatedClickstremDf:DataFrame=dataTypeValidation(clickstreamDf,COL_TIMESTAMP,TO_TIMESTAMP,TIMESTAMP_FORMAT)
    val notNullClickstreamDf:DataFrame=removeNull(validatedClickstremDf,CLICKSTREAM_COLUMNS_CHECK_NULL,INPUT_NULL_CLICKSTREAM_DATA,FILE_TYPE)
    val notNullItemDf:DataFrame=removeNull(itemDf,ITEM_COLUMNS_CHECK_NULL,INPUT_NULL_ITEM_DATA,FILE_TYPE)
    val deduplicatedClickstreamDf:DataFrame=deDuplication(notNullClickstreamDf,COL_TIMESTAMP,SEQ_CLICKSTREAM_PRIMARY_KEYS:_*)
    val deduplicatedItemDf:DataFrame=deDuplication(notNullItemDf,NIL_VALUE,SEQ_ITEM_PRIMARY_KEYS:_*)
    val consistentNameClickstreamDf:DataFrame=consistentNaming(deduplicatedClickstreamDf,REDIRECTION_SOURCE)
    val consistentItemDf:DataFrame=consistentNaming(deduplicatedItemDf,DEPARTMENT_NAME)
    val trimmedClickstreamDf:DataFrame=removeSpaces(consistentNameClickstreamDf,REDIRECTION_SOURCE)
    val trimmedItemDf:DataFrame=removeSpaces(consistentItemDf,DEPARTMENT_NAME)

  }
}
