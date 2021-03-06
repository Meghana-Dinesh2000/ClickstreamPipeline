package com.igniteplus.data.pipeline.cleanseData

import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, desc, initcap, row_number, trim, unix_timestamp, when}

object CleanData
{
  /**
   * Function to remove and filter null values and write null values to separate file
   * @param df the dataframe taken as an input
   * @param primaryColumns sequence of primary key columns
   * @param filePath the location where null values will be written
   * @param fileFormat specifies format of the file
   * @return notNullDf which is the data free from null values
   */
  def filterRemoveNull(df : DataFrame, primaryColumns : Seq[String], filePath : String, fileFormat : String) : DataFrame = {
    var nullDf : DataFrame = df
    var notNullDf : DataFrame = df
    for( i <- primaryColumns)
    {
      nullDf = df.filter(df(i).isNull)
      notNullDf = df.filter(df(i).isNotNull)
    }
    nullDf.count()

    if(nullDf.count() > 0)
      writeFile(nullDf, fileFormat, filePath)
    notNullDf
  }
  /**Alternative for the previous function but not used as the execution time is more in this case*/
  def checkForNull (df : DataFrame, columnNames : Seq[String], filePath : String, fileFormat : String) : DataFrame = {

    val changedColName : Seq[Column] = columnNames.map(x=>col(x))
    val condition:Column=changedColName.map(x=>x.isNull).reduce(_ || _)
    val dfChanged=df.withColumn("nullFlag",when(condition,"true").otherwise("false"))
    val nullDf : DataFrame = dfChanged.filter("nullFlag==true")
    val notNullDf : DataFrame = dfChanged.filter("nullFlag==false").drop("nullFlag")
    if(nullDf.count()>0)
      writeFile(nullDf, fileFormat, filePath)
    notNullDf
  }

  /**
   * Function to remove duplicate values
   * @param df specifies the dataframe to be processed
   * @param keyColumnNames specifies the primary key columns wrt the data
   * @param arrangeColumnName specifies the orderBy if any
   * @return dataframe free from duplicate values
   */
  def deDuplication(df : DataFrame, keyColumnNames : Seq[String], arrangeColumnName : Option[String] = None) : DataFrame =
  {
    arrangeColumnName match{
      case Some(order) =>
        {
          val winSpec = Window.partitionBy(keyColumnNames.head, keyColumnNames.tail: _*)
                  .orderBy(desc(order))
          val deDuplicate: DataFrame = df.withColumn("row_number", row_number().over(winSpec))
                  .filter("row_number==1")
                  .drop("row_number")

          deDuplicate
        }
      case None=>
        {
          val deDuplicate: DataFrame = df.dropDuplicates(keyColumnNames.head, keyColumnNames.tail: _*)
          deDuplicate
        }
    }
  }


  /**
   * Function to trim the datasets
   * @param df is the dataframe to be trimmed
   * @param colName specifies which column should be trimmed
   * @return the dataframe which is trimmed
   */

  def removeSpaces (df : DataFrame, colName : String) : DataFrame = {

    val trimmedDf: DataFrame = df
      // .withColumn("length_without_trimming", functions.length(col(colName)))
      .withColumn(colName, trim(col(colName)))
    // .withColumn("length_with_triming", functions.length(col(colName)))
    //    trimmedDf.show()
    trimmedDf

  }

  /**
   * Function to name the datasets consistently
   * @param df the dataframe to be named consistently
   * @param nameCol column wished to be named consistently
   * @return the dataframe after consistent naming
   */

  def consistentNaming(df : DataFrame, nameCol : String): DataFrame = {

    val consistentNames : DataFrame = df.withColumn(nameCol,initcap(col(nameCol)))
    //consistentNames.show()
    consistentNames


  }

  /**
   * Function to change to the required datatype
   * @param df the dataframe in which datatype should be changed
   * @param colName the column where the datatype needs to be changed
   * @param to_datatype specifies the datatype to which it must be changed to
   * @param format identifies the dataset to be modified
   * @return the modified dataframe
   */

  def dataTypeValidation(df : DataFrame, colName : String, to_datatype : String, format : String) : DataFrame =
  {
    if(format == "nil")
    {
      val dataValidated : DataFrame = df.withColumn(colName, df.col(colName).cast(to_datatype))
      dataValidated
    }
    else
    {
      val dataValidated : DataFrame = df
        .withColumn(colName, unix_timestamp(col(colName), format)
          .cast(to_datatype))
      dataValidated
    }
  }
}







