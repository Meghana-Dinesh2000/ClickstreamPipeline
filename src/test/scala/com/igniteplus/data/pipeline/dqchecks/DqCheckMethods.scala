package com.igniteplus.data.pipeline.dqchecks

import com.igniteplus.data.pipeline.Helper.Helper
import com.igniteplus.data.pipeline.dqchecks.DqCheckMethods.{DqDuplicateCheck, DqNullCheck}
import com.igniteplus.data.pipeline.exception.{DqDuplicateCheckException, DqNullCheckException}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class DqCheckMethodsTest extends AnyFlatSpec with Helper {
  "DqNullCheck() method" should "throw exception if key columns have null values" in {
    import spark.implicits._

    val data = Seq(("H6156", "2020-11-15 09:07:00", "android", null, "C2146", "facebook", "313.5", "J622", "furniture"),
      (null, "2020-11-15 14:39:00", "android", "B000847", "F5021", "google", "1792.0", "E620", "apps & games"),
      ("", "2020-11-15 14:39:00", "android", "B000847", "F5021", "google", "1792.0", "E620", "apps & games"))

    val sampleDF: DataFrame = data.toDF("item_id", "event_timestamp", "device_type", "session_id", "visitor_id", "redirection_source", "item_price", "product_type", "department_name")

    assertThrows[DqNullCheckException] {
      DqNullCheck(sampleDF, PRIMARY_KEY_COLUMNS)
    }
  }

  "DqDuplicateCheck() method" should "throw exception if key columns have duplicate values" in {
    import spark.implicits._

    val data = Seq(("H6156", "2020-11-15 09:07:00", "android", "B000092", "C2146", "facebook", "313.5", "J622", "furniture"),
      ("", "2020-11-15 14:39:00", "android", "B000847", "F5021", "google", "1792.0", "E620", "apps & games"),
      ("H6156", "2020-11-15 09:07:00", "android", "B000092", "C2146", "facebook", "313.5", "J622", "furniture"))

    val sampleDF: DataFrame = data.toDF("item_id", "event_timestamp", "device_type", "session_id", "visitor_id", "redirection_source", "item_price", "product_type", "department_name")

    assertThrows[DqDuplicateCheckException] {
      DqDuplicateCheck(sampleDF, PRIMARY_KEY_COLUMNS, ORDER_BY_COL)
    }
  }


}