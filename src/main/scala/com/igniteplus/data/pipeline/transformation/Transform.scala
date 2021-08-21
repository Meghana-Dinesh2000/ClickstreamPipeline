package com.igniteplus.data.pipeline.transformation

import com.igniteplus.data.pipeline.constants.ApplicationConstants.JOIN_KEY
import org.apache.spark.sql.DataFrame
object Transform
{
    def join(df1 : DataFrame, df2 : DataFrame) : DataFrame = {
      val resultDf : DataFrame = df1.join(df2 ,df1(JOIN_KEY) === df2(JOIN_KEY) , "leftouter")
      resultDf
    }



}
