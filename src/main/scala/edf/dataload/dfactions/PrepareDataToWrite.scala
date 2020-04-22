package edf.dataload.dfactions

import edf.dataload.ddbutilities.writeToDdbTable
import edf.dataload.dfutilities.{HrmnzdDataPull, PiiData, TypeTableJoins}
import edf.dataload.helperutilities.{CdcColumnList, DefineCdcCutOffValues}
import edf.dataload.{considerBatchWindowInd, formatDBName, loadType, piiListMultiMap, restartabilityInd, stgLoadBatch, stgTableMap, auditThroughStreaming, writeToDynamoDB}
import edf.utilities.{Holder, MetaInfo}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.immutable.Map

object PrepareDataToWrite {
  def writeDataFrametoS3(tableToBeIngested: String, propertyConfigs: Map[String, String],
                          metaInfoLookUp: MetaInfo, tableGroup: Map[String, List[String]],
                         saveMode: SaveMode, batchPartition: String, replicationTime: String,
                         hardDeleteBatch: String, hardDeleteDF: DataFrame)
                        (implicit spark: SparkSession) = {
    val ref_col_list = metaInfoLookUp.getRefColList
    val tableDF_arr = tableToBeIngested.split("\\.")
    val formattedDBName = formatDBName(tableDF_arr(0))
    val tableDF = restartabilityInd match {
      case "Y" => formattedDBName + "_" + tableDF_arr(2) + "_" + batchPartition
      case _ => formattedDBName + "_" + tableDF_arr(2) }

    val piiColList = piiListMultiMap.getOrElse(tableToBeIngested, List.empty[String])
    val mainDF_arr = tableToBeIngested.split("\\.")
    val tableKey = if(tableDF.endsWith("_CT") && tableDF.contains("_dbo_"))
      mainDF_arr(0) + ".cdc.dbo_" + mainDF_arr(2) + "_CT"
    else
      tableToBeIngested
    val cdcColMaxStr = CdcColumnList.getCdcColMax(tableKey)
    val considerBatchWindow = if(hardDeleteBatch == "Y") "Y" else considerBatchWindowInd
    val deleteString = if(hardDeleteBatch == "Y") "_delete" else ""
    val tableLoadType = if(stgLoadBatch) stgTableMap.
      getOrElse(tableToBeIngested,"").toUpperCase() else ""

    val (cdcColMax, min_window, max_window) =
      if(auditThroughStreaming || considerBatchWindow == "Y" ||
      (stgLoadBatch && (loadType == "TL" || tableLoadType == "TL") &&
        !cdcColMaxStr._2.endsWith("id")))
      (null,null,null)
    else DefineCdcCutOffValues.getMinMaxCdc(tableDF, tableKey, replicationTime)

    val partitionByCol = cdcColMaxStr._3
    val bucketColumn = if(cdcColMaxStr._2.endsWith("id") ||
      cdcColMaxStr._2.endsWith("yyyymm") ||
      cdcColMaxStr._2.startsWith("loadtime"))
      col("ingestiondt")
    else
      cdcColMaxStr._1

    val dfBeforePii = hardDeleteBatch match {
      case "Y" => spark.sql(s"select * from $tableDF$deleteString")
        .withColumn("ingestiondt", trunc(date_format(
          bucketColumn, "YYYY-MM-dd"), "MM"))
        .withColumn("uniqueId", concat(bucketColumn.cast("Long"),
          col(partitionByCol)))
      case "N"
        if stgLoadBatch && loadType == "TL" => HrmnzdDataPull.getTLDataFromHrmnzd(tableDF_arr(2), piiColList, cdcColMaxStr._2)
      case "N"
        if stgLoadBatch && tableLoadType == "TL" => HrmnzdDataPull.getTLDataFromHrmnzd(tableDF_arr(2), piiColList, cdcColMaxStr._2)
      case "N" if auditThroughStreaming => TypeTableJoins.joinTypeTables(spark, tableToBeIngested, ref_col_list,
        tableGroup, batchPartition)
      case "N" => TypeTableJoins.joinTypeTables(spark, tableToBeIngested, ref_col_list,
        tableGroup, batchPartition).filter(cdcColMax <= max_window)
    }
    val (srcCount, tgtCount) = if(writeToDynamoDB)
                                writeToDdbTable(dfBeforePii, tableDF_arr(2))
                               else
                                PiiData.handlePiiData(dfBeforePii, hardDeleteDF, piiColList,
                                  tableToBeIngested, batchPartition, cdcColMaxStr._2,
                                  saveMode, tableLoadType)

    if( auditThroughStreaming || (stgLoadBatch && (loadType == "TL" || tableLoadType == "TL") &&
          !cdcColMaxStr._2.endsWith("id"))) {
      val window = dfBeforePii.agg(min(cdcColMaxStr._1).as("min_window"),
        max(cdcColMaxStr._1).as("max_window")).rdd.
        map(r => (r.getTimestamp(0), r.getTimestamp(1))).first()
      (srcCount, tgtCount, window._1, window._2)
    }
    else
      (srcCount, tgtCount, min_window, max_window)
  }
}
