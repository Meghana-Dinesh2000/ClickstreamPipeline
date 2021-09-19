package com.igniteplus.data.pipeline.service


import com.igniteplus.data.pipeline.cleanseData.CleanData._
import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import com.igniteplus.data.pipeline.service.DbService.sqlWrite
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.transformation.Transform.join
import org.apache.spark.sql.DataFrame
import org.apache.spark.internal.Logging


object PipelineService extends Logging
{
  def pipelineService () : Unit  =
  {
    /************************************************* Reading Clickstream Data *****************************************/

    val clickstreamDf : DataFrame = readFile(INPUT_LOCATION_CLICKSTREAM,CSV_FILE_TYPE)
    logInfo("Clickstream data read from input location complete.")
//    clickstreamDf.show()
//    clickstreamDf.explain(true)
//    scala.io.StdIn.readLine()

    /************************************************** Reading Item Data ***********************************************/
    val itemDf : DataFrame = readFile(INPUT_LOCATION_ITEM,CSV_FILE_TYPE)
    logInfo("Item data read from input location complete.")
//    itemDf.show()
//    scala.io.StdIn.readLine()

    /**************************************************Changing to appropriate datatype**********************************/
//    clickstreamDf.printSchema()
    val validatedClickstreamDf : DataFrame = dataTypeValidation(clickstreamDf,COL_TIMESTAMP,TO_TIMESTAMP,TIMESTAMP_FORMAT)
    logInfo("Data type validation and conversion complete.")
//  println("Performing Datatype Validation")
//  validatedClickstreamDf.printSchema()
//  scala.io.StdIn.readLine()

    /*********************************Trimming the spaces present in column values***************************************/

    val trimmedClickstreamDf : DataFrame = removeSpaces(validatedClickstreamDf,REDIRECTION_SOURCE)
    val trimmedItemDf : DataFrame = removeSpaces(itemDf,DEPARTMENT_NAME)
    logInfo("Data columns trim complete.")
//    trimmedClickstreamDf.show()
//    trimmedItemDf.show()
//    println("trimmed data")
//    scala.io.StdIn.readLine()


    /*********************************Checking for null vlaues and filtering them****************************************/
    val notNullClickstreamDf : DataFrame = checkForNull(trimmedClickstreamDf,SEQ_CLICKSTREAM_PRIMARY_KEYS,INPUT_NULL_CLICKSTREAM_DATA,CSV_FILE_TYPE)
    val notNullItemDf : DataFrame = checkForNull(trimmedItemDf,SEQ_ITEM_PRIMARY_KEYS,INPUT_NULL_ITEM_DATA,CSV_FILE_TYPE)
    logInfo("Filtering and removing null records from data complete.")
//    notNullClickstreamDf.show()
//    notNullItemDf.show()
//    scala.io.StdIn.readLine()
//    sqlWrite(notNullItemDf,"ITEM_DATA");
//    sqlWrite(notNullClickstreamDf,"CLICKSTREAM_DATA");
//    println("Number of clickstream data rows before deduplication="+notNullClickstreamDf.count())
//    println("Number of item data rows before deduplication="+notNullItemDf.count())

    /*************************************Removing duplicates from data***************************************************/
    val deduplicatedClickstreamDf : DataFrame = deDuplication(notNullClickstreamDf,SEQ_CLICKSTREAM_PRIMARY_KEYS,Some(EVENT_TIMESTAMP))
    val deduplicatedItemDf : DataFrame = deDuplication(notNullItemDf,SEQ_ITEM_PRIMARY_KEYS)
    logInfo("Data de-duplication complete.")
//    deduplicatedClickstreamDf.show()
//    deduplicatedItemDf.show()
//    println("Removal of duplicates")
    //    scala.io.StdIn.readLine()
//    writeFile(deduplicatedClickstreamDf,CSV_FILE_TYPE,"data/Output/DeduplicatedClickstreamData")
//    writeFile(deduplicatedItemDf,CSV_FILE_TYPE,"data/Output/DeduplicatedItemData")
//    println("Number of clickstream data rows after deduplication="+deduplicatedClickstreamDf.count())
//    println("Number of item data rows after deduplication="+deduplicatedItemDf.count())


    /*************************Changing the names to appropriate form by naming them consistently***************************/
    val consistentNameClickstreamDf : DataFrame = consistentNaming(deduplicatedClickstreamDf,REDIRECTION_SOURCE)
    val consistentItemDf : DataFrame = consistentNaming(deduplicatedItemDf,DEPARTMENT_NAME)
    logInfo("Specified data columns conversion to consistent naming  complete.")
//    consistentNameClickstreamDf.show()
//    consistentItemDf.show()
//    println("Converting to lower case")



    /**************************Join Dataframes ****************************************************************************/
    val jointDf : DataFrame = join(consistentNameClickstreamDf, consistentItemDf,JOIN_KEY,JOIN_TYPE)
    logInfo("Clickstream and Item data join complete.")
//    jointDf.show()
//    println("Joining data")
//    scala.io.StdIn.readLine()
//    jointDf.explain()
//    jointDf.show()
//    println(jointDf.count())

    /************************Write to sql staging table ******************************************************************/
    sqlWrite(jointDf,TABLE_NAME,SQL_URL_STAGING)
    logInfo("Transformed data write to staging table complete.")


  }
}
