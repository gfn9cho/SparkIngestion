package edf.dataload.helperutilities

import edf.dataload.auditutilities.{BuildAuditData, FailedTablesFromAuditLog}
import edf.dataload.{considerBatchWindowInd, deleteTableList, hardDeleteBatch, loadType, restartabilityInd, restartabilityLevel, stgLoadBatch, stgTableList, tableIngestionList}
import edf.utilities.Holder
import org.apache.spark.sql.SparkSession

object TableList {
      def getTableList()(implicit spark: SparkSession) = {
        val considerBatchWindow = if(hardDeleteBatch == "Y") "Y" else considerBatchWindowInd
        //val ingestionList = tableIngestionList.sortBy(_.split("\\.")(1))
        val ingestionList = tableIngestionList
        val failedTableAuditMap = if(restartabilityInd == "Y")
                                      FailedTablesFromAuditLog().get else
                                      Map[String, (String, String, String, String)]()
        val auditMap = if(loadType == "TL") Map[String, (String, String, String, String)]()
        else BuildAuditData()
        val tableList =
          if(hardDeleteBatch == "Y") deleteTableList.
                                        map(table => (table, auditMap.getOrElse(table, ("","","",""))))
          else if(stgLoadBatch && restartabilityInd != "Y") stgTableList.
                                        map(table => (table._1, auditMap.getOrElse(table._1, ("","","",""))))
          else if(stgLoadBatch && restartabilityInd == "Y") {
            if(restartabilityLevel == "table") failedTableAuditMap
            else {
              val  part = failedTableAuditMap.toList match {
                case head :: _ =>  head._2._2
                case _ => ""
              }
              (stgTableList.map(_._1) diff failedTableAuditMap.keys.toList).
              map(table => (table, auditMap.getOrElse(table, ("", part, "", ""))))
            }
          }
          else if(restartabilityInd == "Y") {
            if(restartabilityLevel == "table") failedTableAuditMap
            else if(considerBatchWindow == "Y") {
              val (start, end, part) = failedTableAuditMap.toList match {
                case head :: _ => (head._2._3, head._2._4, head._2._2)
                case _ => (null, null, "")
              }
              (ingestionList diff failedTableAuditMap.keys.toList).
                map(table => (table, (null, part, start, end) ))
            } else
              (ingestionList diff failedTableAuditMap.keys.toList).
                map(table => (table, auditMap.getOrElse(table, ("","","",""))))
            }
        else ingestionList.
                  map(table => (table, auditMap.getOrElse(table, ("","","",""))))
        //Holder.log.info("tableList:" + tableList.toList.mkString(","))
        tableList.toList
      }

  def apply()(implicit spark: SparkSession) = {
    getTableList()
  }
}
