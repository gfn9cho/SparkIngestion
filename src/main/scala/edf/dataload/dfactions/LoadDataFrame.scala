package edf.dataload.dfactions

import edf.dataload.helperutilities.CdcColumnList.getCdcColMax
import edf.dataload.{hardDeleteBatch, loadType, now, refTableList, restartabilityInd, splitString, stgLoadBatch}
import edf.dataload.dfutilities.EmptyDataFrameCheck
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.util.{Failure, Success}

object LoadDataFrame {
  def processCompletedFutures(ingestionResult: (String, String), replicationTime: String,
                              tableWithAuditInfo: (String, (String, String, String, String)), hardDeleteDF: DataFrame)
                             (implicit spark: SparkSession) = {
    val writeFutures = {
      val auditStr = ingestionResult._2.split(splitString)
      val readStatistics = Row(auditStr(0), auditStr(1), auditStr(2), auditStr(3).toLong, auditStr(4), auditStr(5), auditStr(6), auditStr(7), auditStr(8), auditStr(9), auditStr(10), auditStr(11), auditStr(12))
      //val tableToIngest = restartabilityInd match {case "Y" => auditStr(2) + "_" + auditStr(1) case _ => auditStr(2)}
      val tableName = auditStr(2)
      val cdcCol = getCdcColMax(tableName)
      val batchPartition = auditStr(1)
      val mainDF_arr = tableName.split("\\.")
      val databaseName = if (mainDF_arr(0).contains("-"))
        mainDF_arr(0).replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("-", "_")
      else
        mainDF_arr(0)
      val mainDF = restartabilityInd match {
        case "Y" => databaseName + "_" + mainDF_arr(2) + "_" + batchPartition
        case "N" if hardDeleteBatch == "Y" => databaseName + "_" + mainDF_arr(2) + "_delete"
        case "N" => databaseName + "_" + mainDF_arr(2)
      }
      val saveMode = if (loadType == "TL" || restartabilityInd == "Y")
        SaveMode.Overwrite else SaveMode.Append
      val auditInfo = tableWithAuditInfo._2
      //Get the previous window timing to log into audit incase of Failure
      //For Success scenario, window timing will be based on current data
      //FOr Failure scenario, capture previous window for restartability
      val prevWindow = if (loadType != "TL") (auditInfo._3, auditInfo._4)
                        else if (cdcCol._2.endsWith("id") ||
                                  cdcCol._2.endsWith("yyyymm") ||
                                    cdcCol._2.startsWith("loadtime")) ("0", "0")
                              else ("1900-01-01", "1900-01-01")
      def isEmptyDF = EmptyDataFrameCheck(mainDF_arr(2), mainDF)
      ingestionResult._1 match {
        case "success" =>
          val loadStartTime = now.toString("YYYY-MM-dd HH:mm:ss.sss")
          /*
        * EDIN-362: Create empty hive table in harmonized layer for TL
        */
          val loadStats =
          //if (!emptyDataFrameInd) s3WriteIterator(0) else Success((0L, 0L, "success", "",prevWindow._1,prevWindow._2))
            if (loadType == "TL" || stgLoadBatch  || !isEmptyDF)
              WriteInitiator(tableName, saveMode, batchPartition, replicationTime,
                auditStr(8), auditStr(9), prevWindow, 0, hardDeleteDF)
            else Success((0L, 0L, "success", "", prevWindow._1, prevWindow._2))
          //dropping the temp view for the main table.

          if (spark.catalog.tableExists(mainDF)) {
            if (!refTableList.contains(tableName)) {
              spark.catalog.dropTempView(mainDF)
            }
          }
          loadStats match {
            case Success(stats) =>
              val minWindowStr = if (stats._5 == null) "" else stats._5.toString
              val maxWindowStr = if (stats._6 == null) "" else stats._6.toString
              val (srcCount, dfCount, loadStatus, ex, minWindow, maxWindow) = (stats._1, stats._2, stats._3, stats._4, minWindowStr, maxWindowStr)
              ("success", Row.fromSeq(readStatistics.toSeq :+
                dfCount :+
                hardDeleteBatch
                patch(3, Seq(srcCount), 1)
                patch(4, Seq(loadStartTime), 1)
                patch(5, Seq(now.toString("YYYY-MM-dd HH:mm:ss.sss")), 1)
                patch(6, Seq(loadStatus), 1)
                patch(7, Seq(ex), 1)
                patch(8, Seq(minWindow), 1)
                patch(9, Seq(maxWindow), 1)))
            case Failure(ex) =>
              val exStr = ex.getMessage.split(splitString)
              val (srcCount, dfCount, loadStatus, error, minWindow, maxWindow) = (exStr(0).toLong, exStr(1).toLong, exStr(2), exStr(3), exStr(4), exStr(5))
              val auditData = Row.fromSeq(readStatistics.toSeq :+
                dfCount :+
                hardDeleteBatch
                patch(3, Seq(srcCount), 1)
                patch(4, Seq(loadStartTime), 1)
                patch(5, Seq(now.toString("YYYY-MM-dd HH:mm:ss.sss")), 1)
                patch(6, Seq(loadStatus), 1)
                patch(7, Seq(error), 1)
                patch(8, Seq(minWindow), 1)
                patch(9, Seq(maxWindow), 1))
              ("failed", auditData)
          }
        case "failed" => ("failed", Row.fromSeq(readStatistics.toSeq :+ 0L :+ hardDeleteBatch))
      }
    }
    writeFutures
  }
  def apply(ingestionResult: (String, String), replicationTime: String,
            tableWithAuditInfo: (String, (String, String, String, String)),
            hardDeleteDF: DataFrame)
           (implicit spark: SparkSession) = {
    processCompletedFutures(ingestionResult, replicationTime,
      tableWithAuditInfo, hardDeleteDF)
  }
}
