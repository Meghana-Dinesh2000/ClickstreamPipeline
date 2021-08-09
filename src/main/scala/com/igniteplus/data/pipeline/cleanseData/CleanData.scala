package com.igniteplus.data.pipeline.cleanseData

import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, desc, row_number, trim, when}

object CleanData
{
//  def removeNull (df : DataFrame, columnName : Seq[String], filePath : String, fileFormat : String) : DataFrame = {
//    var nullDf : DataFrame = df
//    var notNullDf : DataFrame = df
//    for( i <- columnName)
//    {
//      nullDf = df.filter(df(i).isNull)
//      notNullDf = df.filter(df(i).isNotNull)
//    }
//    if(nullDf.count() > 0)
//      writeFile(nullDf, fileFormat, filePath)
//    notNullDf
//  }
  /**Alternative for the previous function but not used as the execution time is more in this case*/
  def checkForNull (df : DataFrame, columnNames : Seq[String], filePath : String, fileFormat : String) : DataFrame =
  {
    val changedColName : Seq[Column] = columnNames.map(x=>col(x))
    val condition:Column=changedColName.map(x=>x.isNull).reduce(_ || _)
    val dfChanged=df.withColumn("nullFlag",when(condition,"true").otherwise("false"))
    val nullDf : DataFrame = dfChanged.filter("nullFlag==true")
    val notNullDf : DataFrame = dfChanged.filter("nullFlag==false")
    if(nullDf.count()>0)
      writeFile(nullDf, fileFormat, filePath)
    notNullDf
  }

  /**Function to remove duplicates*/
  def deDuplication(df : DataFrame, orderByColumn : String, colNames : String*) : DataFrame =
  {
    if (orderByColumn == "nil") {
      val deDuplicate: DataFrame = df.dropDuplicates(colNames.head, colNames.tail: _*)
      deDuplicate
    }
    else {
      val winSpec = Window.partitionBy(colNames.head, colNames.tail: _*)
        .orderBy(desc(orderByColumn))
      val deDuplicate: DataFrame = df.withColumn("row_number", row_number().over(winSpec))
        .filter("row_number==1")
        .drop("row_number")
      deDuplicate
    }
  }


  /**Function used to trim the given column values*/

  def removeSpaces (df : DataFrame, colName : String) : DataFrame =
  {
    val trimmedDf: DataFrame = df
      // .withColumn("length_without_trimming", functions.length(col(colName)))
      .withColumn(colName, trim(col(colName)))
    // .withColumn("length_with_triming", functions.length(col(colName)))
    //    trimmedDf.show()
    trimmedDf
  }
}







