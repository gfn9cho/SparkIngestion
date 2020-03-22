package edf.dataload.auditutilities

import edf.dataload.{auditDB, auditPath, processName, schema, splitString}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.immutable.Map

object AuditLog {
  def logAuditRow(stats: (String, Row), batchWindowStart: String, replTime: String)
                 (implicit spark: SparkSession) = {
    {

      val auditFrame = spark.createDataFrame(spark.sparkContext.parallelize(Seq(stats._2),1), schema)
      val auditSaveMode = if(spark.catalog.tableExists(s"$auditDB.audit"))
        SaveMode.Append else SaveMode.Overwrite
      auditFrame
        .filter(!col("batchwindowend").isNull)
        .withColumn("processname", lit(processName))
        .write.format("parquet")
        .partitionBy("processname", "ingestiondt")
        .options(Map("path" -> (auditPath + "/audit")))
        .mode(auditSaveMode).saveAsTable(s"$auditDB.audit")

      val auditData = stats._1 match {
        case "failed" => Some(stats._2.mkString(splitString))
        case "success" => None
      }

      (auditData, batchWindowStart, replTime, stats._2.getAs[String](12), stats._2.getAs[String](1), stats._2.getAs[String](14))
    }
  }

  def apply(stats: (String, Row), batchWindowStart: String, replTime: String)
           (implicit spark: SparkSession) = {
    logAuditRow(stats,batchWindowStart,replTime)
  }
}
