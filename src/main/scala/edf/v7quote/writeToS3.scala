package edf.v7quote

import edf.utilities.Holder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.immutable.Map

object writeToS3 {

  def writeToS3(table: String, targetLocation: String,
                hiveTable: String, df: DataFrame)(implicit spark: SparkSession) = {
    //val s3Location = s"$targetLocation/history/$table"
    df.
      write.format("parquet").
      partitionBy("ingestiondt", "batch").
      options(Map("path" -> targetLocation)).
      mode(SaveMode.Overwrite).
      saveAsTable(hiveTable)
    spark.sql(s"select itemid from $hiveTable where batch = '9999999999999' ")
  }

  def piiDataClassification(table: String, df: DataFrame, piiColList: List[String])
                           (implicit spark: SparkSession): DataFrame = {
    piiColList match {
      case pii: List[String]
        if pii.isEmpty =>
        val hiveTable = s"$targetDB.$table"
        writeToS3(table, s"$hrmnzds3Path/$table", hiveTable, df )
      case pii: List[String] => {
        val hiveTable = s"$targetSecuredDB.$table"
        writeToS3(table, s"$harmonizeds3SecurePath/$table", hiveTable, df)
        val dfUpdated: DataFrame = pii.foldLeft(df)((d, c) => d.withColumn(c, lit("")))
        piiDataClassification(table, dfUpdated, List.empty[String])
      }
    }
  }
  def apply(table: String)(df: DataFrame)(implicit spark: SparkSession) = {
    val piiColList = piiListMultiMap.map(list => (list._1.split("\\.")(2), list._2)).getOrElse(table, List.empty[String])
    piiDataClassification(table, df, piiColList)
  }
}
