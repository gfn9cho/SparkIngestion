package edf.missingdataload

import edf.dataingestion.{harmonizeds3SecurePath, hrmnzds3Path, now, processName, targetDB, targetSecuredDB, timeZone}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.joda.time.format.DateTimeFormat

import scala.collection.immutable.Map

object IngestData {
  def ingestMissingRecords(finalMissingDataSet: (String, DataFrame, String, Any, List[String], String))
                          (implicit spark: SparkSession): Row = {
    val table = finalMissingDataSet._1
    val missingData = finalMissingDataSet._2
    val missingIds = finalMissingDataSet._3
    val cutOffValue = finalMissingDataSet._4
    val piiColList = finalMissingDataSet._5
    val missingDataBatch = finalMissingDataSet._6

    import org.apache.spark.sql.functions._

    def saveDF(df: DataFrame, hiveTable: String, s3Path: String): Unit = {
      df.
        //withColumn("updatetime", lit(current_timestamp())).
        write.format("parquet").
        partitionBy("ingestiondt", "batch")
        .options(Map("path" -> s3Path, "maxRecordsPerFile" -> "100000"))
        .mode(SaveMode.Append)
        .saveAsTable(hiveTable)
    }

    def ingestDataIntoHrmnzd(df: DataFrame, piiColList: List[String]): Unit = piiColList match {
      case pii: List[String] if pii.isEmpty =>
        saveDF(df, s"$targetDB." + table, hrmnzds3Path + "/" + table)
      case pii: List[String] =>
        val piiData = pii.foldLeft(df)((d, c) => d.withColumn(c, lit("")))
        saveDF(df, s"$targetSecuredDB." + table, harmonizeds3SecurePath + "/" + table)
        ingestDataIntoHrmnzd(piiData, List.empty[String])
    }

    ingestDataIntoHrmnzd(missingData, piiColList)
    val (ingestiondt: String, batch: String) = missingData.select(col("ingestiondt").cast("String"), col("batch")).rdd.map(row => (row.getString(0), row.getString(1))).first

    val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.sss")
    val current_time = datetime_format.
      parseDateTime(now.toString("YYYY-MM-dd HH:mm:ss.sss")).
      withZone(timeZone)
    Row(processName, table, missingIds, cutOffValue.toString, ingestiondt, batch, current_time.toString, "Y", missingDataBatch)
  }

  def apply(missingDataSet: (String, DataFrame, String, Any, List[String], String))
           (implicit spark: SparkSession)={
    ingestMissingRecords(missingDataSet)
  }
}
