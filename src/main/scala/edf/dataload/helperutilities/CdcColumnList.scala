package edf.dataload.helperutilities

import edf.dataload.{splitString, tableSpecMap}

object CdcColumnList {
  def getCdcColMax(tableToBeIngested: String) = {
    val cdcColFromTableSpecStr = tableSpecMap.getOrElse(tableToBeIngested, "").split(splitString, -1)
    val (cdcColFromTableSpec, hardDeleteFlag, partitionBy) = (cdcColFromTableSpecStr(0),
      cdcColFromTableSpecStr(1),
      cdcColFromTableSpecStr(2) )
    val coalescePattern = """coalesce\((.*),(.*)\)""".r
    val multiColPattern = """(.*,.*)+""".r
    import org.apache.spark.sql.functions.{coalesce, col}

    val (cdcColMax, colName) = cdcColFromTableSpec match {
      case coalescePattern(col1: String, col2: String) => {
        (coalesce(col(col1), col(col2)), col1)
      }
      case multiColPattern(cols: String) => {
        //TODO partition for mutli cdc pattern needs to be handled
        (col("dummy"),null)
      }
      case clmn: String => (if (hardDeleteFlag == "cdc" && tableToBeIngested.contains(".cdc."))
        col("deleteTime") else col(clmn), null)
    }
    (cdcColMax, cdcColFromTableSpec, partitionBy, colName)
  }
}
