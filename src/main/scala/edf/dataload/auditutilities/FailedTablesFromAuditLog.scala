package edf.dataload.auditutilities

import edf.dataload.{auditDB, processName, restartTableIdentifier, restartabilityInd, restartabilityLevel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

import scala.collection.immutable.Map

object FailedTablesFromAuditLog {
  def getFailedTables()(implicit spark: SparkSession) = {
    restartabilityInd match {
      case "Y" => {
        val failedTable = restartabilityLevel match {
          case "table" =>
            val auditMapStr = spark.sql(s"select a.tableName, a.ingestiondt, a.batch, " +
            s"a.batchwindowstart, a.batchwindowend, a.loadstatus from $auditDB.audit a " +
            s"where a.processname = '${processName}' " +
            s"and a.ingestiondt in (select max(ingestiondt) from $auditDB.audit where processname = '$processName')")
            .filter(col("loadstatus") === s"$restartTableIdentifier")
            .groupBy(col("tableName"))
            .agg(
              max(col("ingestiondt")).as("ingestiondt"),
              max(col("batch")).as("batch"),
              max(col("batchwindowstart")).as("batchwindowstart"),
              max(col("batchwindowend")).as("batchwindowend")
            )
            .rdd.flatMap(row => Map(row.getAs[String](0)-> (row.getAs[String](1),
            row.getAs[String](2), row.getAs[String](3),row.getAs[String](4))))
            .collect
            val auditMap = auditMapStr.map(arrValue => arrValue._1 -> arrValue._2).toMap
            auditMap
          case "batch" => {
            val lastFailedBatch = spark.sql(s"select  max(batch) as batch from $auditDB.audit " +
              s"where processname = '$processName' ").first().getAs[String](0)

            val auditMapStr = spark.sql(s"select a.tableName, a.ingestiondt, a.batch, " +
              s"a.batchwindowstart, a.batchwindowend from $auditDB.audit a " +
              s"where batch='$lastFailedBatch' and batchwindowend != 'null'")
              .groupBy(col("tableName"))
              .agg(
                max(col("ingestiondt")).as("ingestiondt"),
                max(col("batch")).as("batch"),
                max(col("batchwindowstart")).as("batchwindowstart"),
                max(col("batchwindowend")).as("batchwindowend")
              )
              .rdd.flatMap(row => Map(row.getAs[String](0)-> (row.getAs[String](1),
              row.getAs[String](2), row.getAs[String](3),row.getAs[String](4))))
              .collect
            val auditMap = auditMapStr.map(arrValue => arrValue._1 -> arrValue._2).toMap
            auditMap
          }
        }
        Some(failedTable)
      }
      case _ => None
    }
  }

  def apply()(implicit spark: SparkSession) = {
    getFailedTables()
  }
}
