package com.igniteplus.data.pipeline.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, initcap, unix_timestamp}

object Transform
{
    def innerJoin(df1 : DataFrame , df2 : DataFrame) : DataFrame = {
      val resultDf : DataFrame = df1.join(df2 , df1("item_id")=== df2("item_id"), "leftouter")
      resultDf
    }



}
