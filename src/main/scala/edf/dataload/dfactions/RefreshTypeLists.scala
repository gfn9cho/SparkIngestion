package edf.dataload.dfactions

import edf.dataload.{auditDB, hardDeleteBatch, isConnectDatabase, isTypeListToBeRefreshed, loadType, mainTableListFromTableSpec, oldConsolidateTypeList, propertyMap, refTableListStr, restartabilityInd, splitString}
import edf.dataload.auditutilities.BuildAuditData
import edf.dataload.dfutilities.ReadTable
import edf.utilities.BatchConcurrency
import org.apache.spark.sql.SparkSession

object RefreshTypeLists {
  def refreshTypeList(btchWdwStartTime: String, replTime: String)
                     (implicit spark: SparkSession)= {
    if (refTableListStr.trim != "")
      if ((isConnectDatabase && !isTypeListToBeRefreshed) ||
        (restartabilityInd == "Y" && isConnectDatabase))
        spark.sql(s"select * from $auditDB.$oldConsolidateTypeList")
          .coalesce(1)
          .createOrReplaceTempView(oldConsolidateTypeList)
      else if (hardDeleteBatch == "N")
        writeTypeListTables(btchWdwStartTime, replTime)
    val auditMap = if ((loadType == "DI" && hardDeleteBatch == "N") || loadType == "CI")
      Some(BuildAuditData()) else None
    auditMap
  }

  def writeTypeListTables(batchWindowStart: String, replicationTime: String)
                         (implicit spark: SparkSession)= {
    val typeListIngestion = refTableListStr.split(splitString).par.map(table =>
      BatchConcurrency.executeAsync(ReadTable(table, 'Y', batchWindowStart, null, null, null))).toIterator
    if(isConnectDatabase){
      if(isTypeListToBeRefreshed) {
        WriteTypeList.writeConnectTypeListTables(spark, propertyMap,
          typeListIngestion, mainTableListFromTableSpec)
      }
    } else {
      WriteTypeList.writeOtherTYpeListTables(spark, propertyMap,
        typeListIngestion, mainTableListFromTableSpec)
    }
  }

  def apply(btchWdwStartTime: String, replTime: String)
           (implicit spark: SparkSession) =
    refreshTypeList(btchWdwStartTime, replTime)
}
