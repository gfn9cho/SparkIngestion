package edf.missingdataload

import edf.dataingestion.{splitString, tableSpecMapTrimmed, targetDB}
import edf.utilities.Holder
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.functions.{col, max}

object MissingRecords {
  def identifyMissingRecords(dfReaderWithTable: (String, DataFrameReader), maxData: DataFrame)
                            (implicit spark: SparkSession, missingDataBatch: String): (String, (String, Any, String)) = {
    val table = dfReaderWithTable._1
    val dfReader = dfReaderWithTable._2
    val cdcColFromTableSpecStr = tableSpecMapTrimmed.getOrElse(table, "").split(splitString, -1)
    val partitionByCol = cdcColFromTableSpecStr(2).trim
    val cdcCol = cdcColFromTableSpecStr(0).trim
    val sourceData = dfReader.load()

    val targetData = if (partitionByCol == cdcCol)
      spark.sql(s"select $partitionByCol from $targetDB.$table")
       // .where(!col(partitionByCol).isin(707,708))
        //.repartitionByRange(20, col(s"$partitionByCol"))
    else
      spark.sql(s"select $partitionByCol, $cdcCol from $targetDB.$table")
       // .where(!col(partitionByCol).isin(707,708))
        //.repartitionByRange(20, col(s"$partitionByCol"))
        //.groupBy(partitionByCol).agg(max(cdcCol).as(cdcCol))
import org.apache.spark.sql.functions.lit
    /*    val maxData = spark.sql(s"select tableName, batchwindowend, harddeletebatch from edf_dataingestion.audit " +
          s"where processName='gwplDataExtract' ").
          where(col("harddeletebatch")===lit('N')).
          groupBy(col("tableName")).agg(max(col("batchwindowend"))).cache*/
// and tableName='policycenter.dbo.$table'
    val maxRecordInDataLake =
      if (partitionByCol == cdcCol)
        //targetData.agg(max(partitionByCol)).first().getLong(0)
        maxData.where(col("tableName")===lit(s"policycenter.dbo.$table")).first().getString(1)
      else
        //targetData.agg(max(cdcCol)).first().getTimestamp(0)
        maxData.where(col("tableName")===lit(s"policycenter.dbo.$table")).first().getString(1)

    //TODO need to change this logic
    val sourceDataFiltered = sourceData.where(col(cdcCol) <= maxRecordInDataLake)
    //  sort("id").limit(1000)

    /*val missingSourceRecords = sourceDataFiltered.except(targetData).rdd.map { row =>
      row.getLong(0)
    }.collect.take(200).mkString(",")*/
    val missingSourceRecords = sourceDataFiltered.except(targetData).take(500).map(row => row.getLong(0)).mkString(",")
    Holder.log.info("missingSourceRecords: " + missingSourceRecords + " : " + maxRecordInDataLake)
    (table, (missingSourceRecords, maxRecordInDataLake, missingDataBatch))
  }

  def apply(dfReader: (String, DataFrameReader), maxData: DataFrame)(implicit spark: SparkSession, batch: String ) = {
    identifyMissingRecords(dfReader, maxData)
  }
}
