package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import com.igniteplus.data.pipeline.dqchecks.DqCheckMethods
import com.igniteplus.data.pipeline.service.DbService.sqlWrite
import org.apache.spark.sql.DataFrame
import sun.java2d.marlin.MarlinUtils.logInfo

object DqCheckService {
  def executeDqCheck() : Unit = {


    /*********************************** READING THE STAGED TABLE FROM MYSQL***********************************************************************/
    val dfReadStaged : DataFrame = DbService.sqlRead(TABLE_NAME,SQL_URL_STAGING)
    logInfo("Data read from staging table completed.")
    /*********************************** CHECK NULL VALUES***********************************************************************/
    val dfCheckNull : Boolean = DqCheckMethods.DqNullCheck(dfReadStaged,COLUMNS_CHECK_NULL_DQ_CHECK)
    logInfo("Data quality null check completed.")

    /***********************************CHECK DUPLICATE VALUES***********************************************************************/
    val dfCheckDuplicate : Boolean = DqCheckMethods.DqDuplicateCheck(dfReadStaged,SEQ_CLICKSTREAM_PRIMARY_KEYS,EVENT_TIMESTAMP)
    logInfo("Data quality duplicate check completed.")
    /*********************************** WRITING TO PROD TABLE IN MYSQL***********************************************************************/
    if(dfCheckNull && dfCheckDuplicate){
      sqlWrite(dfReadStaged,TABLE_NAME,SQL_URL_PROD)
      logInfo("Data write to production table complete.")
//      scala.io.StdIn.readLine()
    }
  }


}
