package com.igniteplus.data.pipeline
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{APP_NAME, CLICKSTREAM_DATA, CONSISTENT_NAMES, DEDUPLICATED_DATA, INPUT_LOCATION_CLICKSTREAM, INPUT_LOCATION_ITEM, ITEM_DATA, MASTER_NAME, VALIDATED_DATA}
import com.igniteplus.data.pipeline.cleanseData.consistentNaming.consistentNaming
import com.igniteplus.data.pipeline.cleanseData.spaceRemoval.removeSpaces
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ProductName
{
  def main(args: Array[String]): Unit =
  {
//    println("Reading Clickstream Data:")
//    CLICKSTREAM_DATA.show()
//    println("Reading Item Data:")
//    ITEM_DATA.show()
//    println("Schema of clickstream data before datatype validation:")
//    CLICKSTREAM_DATA.printSchema()
//    println("Schema of item Data:")
//    ITEM_DATA.printSchema()
//    println("Schema of clickstream Data after datatype validation:")
//    VALIDATED_DATA.printSchema()
//    println("Clickstream Data after de-duplication:")
//    DEDUPLICATED_DATA.show()
//    println("Number of rows before de-duplication:"+VALIDATED_DATA.count())
//    println("Number of rows after de-duplication:"+DEDUPLICATED_DATA.count())
//    println("ClickStream Data after consistent naming of redirection_source")
//    CONSISTENT_NAMES.show()
    println("Trimmed Data after removing spaces:")
    val df:DataFrame=removeSpaces(ITEM_DATA,ITEM_DATA(col("department_name").toString()))

    df.show()

  }
}
