package com.igniteplus.data.pipeline.service

import com.google.common.io.BaseEncoding
import com.igniteplus.data.pipeline.constants.ApplicationConstants._
import org.apache.spark.sql.DataFrame
import java.io.FileInputStream
import java.security.{Key, KeyStore}
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

object DbService
{
  def encryptUsingAESKey(data: String, key: Array[Byte]) : Unit =
    {
      val secKey:SecretKeySpec=new SecretKeySpec(key,"AES")
      val cipher : Cipher = Cipher.getInstance("AES")
      cipher.init(Cipher.ENCRYPT_MODE,secKey)
      val newBytes:Array[Byte]=cipher.doFinal(data.getBytes())
      val base64EncodedEncryptedMsg = BaseEncoding.base64.encode(newBytes)
      scala.tools.nsc.io.File(LOCATION_ENCRYPTED_PASSWORD).writeAll(base64EncodedEncryptedMsg)
    }

  def decryptUsingAESKey(encryptedData: String, key: Array[Byte]) : String = {
    val secKey : SecretKeySpec = new SecretKeySpec(key,"AES")
    val cipher : Cipher = Cipher.getInstance("AES")
    cipher.init(Cipher.DECRYPT_MODE,secKey)
    val newData : Array[Byte] =cipher.doFinal(BaseEncoding.base64().decode(encryptedData))
    val message : String = new String(newData)
    message
  }
  def securityEncryptionDecryption(): String = {
    val keyStore : KeyStore = KeyStore.getInstance("JCEKS");
    val stream : FileInputStream = new FileInputStream("mykeystore.jks")
    keyStore.load(stream,KEY_PASSWORD.toCharArray)
    val key : Key = keyStore.getKey("mykey",KEY_PASSWORD.toCharArray)
    val source = scala.io.Source.fromFile(LOCATION_SQL_PASSWORD)
    val data : String = source.mkString
    encryptUsingAESKey(data,key.getEncoded)
    val encryptedData : String = scala.io.Source.fromFile(LOCATION_ENCRYPTED_PASSWORD).mkString
    val decryptedData = decryptUsingAESKey(encryptedData,key.getEncoded)
    decryptedData
  }

  def sqlWrite(df : DataFrame, tableName : String) : Unit = {
        /**Method -1*/
//    val url = SQL_URL
//    df.write.format("jdbc")
//      .mode("overwrite")
//      .option("url",url)
//      .option("dbtable",tableName)
//      .option("user",USER_NAME)
//      .option("password",securityEncryptionDecryption())
//      .save()
    /** Method-2 */
    val prop = new java.util.Properties
    prop.setProperty("driver", JDBC_DRIVER)
    prop.setProperty("user", USER_NAME)
    prop.setProperty("password", securityEncryptionDecryption())
    val url = SQL_URL
    df.write.mode("overwrite").jdbc(url, tableName, prop)
  }
}