package com.igniteplus.data.pipeline.exception

class ApplicationException (message :String)extends Exception(message)


case class FileReaderException(message: String) extends ApplicationException(message:String)
case class FileWriterException(message: String) extends  ApplicationException(message:String)
case class DqDuplicateCheckException(message: String) extends ApplicationException(message:String)
case class DqNullCheckException(message: String) extends ApplicationException(message:String)