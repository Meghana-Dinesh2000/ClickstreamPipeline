package com.igniteplus.data.pipeline.service


import com.igniteplus.data.pipeline.cleanseData.CleanData._
import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import com.igniteplus.data.pipeline.service.DbService.sqlWrite
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import com.igniteplus.data.pipeline.transformation.Transform.join
import org.apache.spark.sql.DataFrame


object PipelineService
{
  def pipelineService () : Unit  =
  {
    /************************************************* Reading Clickstream Data *****************************************/
    val clickstreamDf : DataFrame = readFile(INPUT_LOCATION_CLICKSTREAM,CSV_FILE_TYPE)
    println("total rows in clickstream data=",clickstreamDf.count)


    /************************************************** Reading Item Data ***********************************************/
    val itemDf : DataFrame = readFile(INPUT_LOCATION_ITEM,CSV_FILE_TYPE)
    println("total rows in item data=",itemDf.count)

    /**************************************************Changing to appropriate datatype**********************************/
    val validatedClickstreamDf : DataFrame = dataTypeValidation(clickstreamDf,COL_TIMESTAMP,TO_TIMESTAMP,TIMESTAMP_FORMAT)
    println("data of clickstream after change datatype=",validatedClickstreamDf.count())

    /*********************************Trimming the spaces present in column values***************************************/
    val trimmedClickstreamDf : DataFrame = removeSpaces(validatedClickstreamDf,REDIRECTION_SOURCE)
    val trimmedItemDf : DataFrame = removeSpaces(itemDf,DEPARTMENT_NAME)
    println("Number of rows in clickstream data after trimming:",trimmedClickstreamDf.count)
    println("Number of rows in item data after trimming:",trimmedItemDf.count)


    /*********************************Checking for null vlaues and filtering them****************************************/
    val notNullClickstreamDf : DataFrame = checkForNull(trimmedClickstreamDf,SEQ_CLICKSTREAM_PRIMARY_KEYS,INPUT_NULL_CLICKSTREAM_DATA,CSV_FILE_TYPE)
    val notNullItemDf : DataFrame = checkForNull(trimmedItemDf,SEQ_ITEM_PRIMARY_KEYS,INPUT_NULL_ITEM_DATA,CSV_FILE_TYPE)
    println("Number of rows in clickstream data after null removal:",notNullClickstreamDf.count)
    println("Number of rows in item data after removing null:",notNullItemDf.count)
//    sqlWrite(notNullItemDf,"ITEM_DATA");
//    sqlWrite(notNullClickstreamDf,"CLICKSTREAM_DATA");
//    println("Number of clickstream data rows before deduplication="+notNullClickstreamDf.count())
//    println("Number of item data rows before deduplication="+notNullItemDf.count())

    /*************************************Removing duplicates from data***************************************************/
    val deduplicatedClickstreamDf : DataFrame = deDuplication(notNullClickstreamDf,SEQ_CLICKSTREAM_PRIMARY_KEYS,Some(EVENT_TIMESTAMP))
    val deduplicatedItemDf : DataFrame = deDuplication(notNullItemDf,SEQ_ITEM_PRIMARY_KEYS)
    println("Number of rows in clickstream data after duplicates removal:",deduplicatedClickstreamDf.count)
    println("Number of rows in item data after duplicates null:",deduplicatedItemDf.count)
//    writeFile(deduplicatedClickstreamDf,CSV_FILE_TYPE,"data/Output/DeduplicatedClickstreamData")
//    writeFile(deduplicatedItemDf,CSV_FILE_TYPE,"data/Output/DeduplicatedItemData")
//    println("Number of clickstream data rows after deduplication="+deduplicatedClickstreamDf.count())
//    println("Number of item data rows after deduplication="+deduplicatedItemDf.count())


    /*************************Changing the names to appropriate form by naming them consistently***************************/
    val consistentNameClickstreamDf : DataFrame = consistentNaming(deduplicatedClickstreamDf,REDIRECTION_SOURCE)
    val consistentItemDf : DataFrame = consistentNaming(deduplicatedItemDf,DEPARTMENT_NAME)
    println("Number of rows in clickstream data after naming:",consistentNameClickstreamDf.count)
    println("Number of rows in item data after naming:",consistentItemDf.count)

    /**************************Join Dataframes ****************************************************************************/
    val jointDf : DataFrame = join(consistentNameClickstreamDf, consistentItemDf,JOIN_KEY,JOIN_TYPE)
    println("Number of rows after join:",jointDf.count)
//    jointDf.explain()
//    jointDf.show()
//    println(jointDf.count())

    /************************Write to sql staging table ******************************************************************/
    sqlWrite(jointDf,TABLE_NAME,SQL_URL_STAGING)
//
  }
}
