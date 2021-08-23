package com.igniteplus.data.pipeline.transformation

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{JOIN_KEY, JOIN_TYPE}
import org.apache.spark.sql.DataFrame
object Transform
{
    def join(df1 : DataFrame, df2 : DataFrame, joinKey : String, joinType : String) : DataFrame = {
      val jointDf : DataFrame = df1.join(df2 ,Seq(joinKey), joinType)
      jointDf
    }
}
