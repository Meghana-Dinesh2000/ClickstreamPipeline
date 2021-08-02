package com.igniteplus.data.pipeline.util
import org.apache.spark.sql.SparkSession

object ApplicationUtil
{
  /** Defining a spark session */
  def sparkSessionCreate(masterName:String,applicationName:String):SparkSession =
  {
    implicit val spark:SparkSession = SparkSession
      .builder
      .master(masterName)
      .appName(applicationName)
      .getOrCreate()

    spark
  }
}
