package edf.dataload.auditutilities

import edf.dataload.{auditDB, processName}
import edf.utilities.Holder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.immutable.Map

object BuildAuditData {
  def buildAuditData()(implicit spark: SparkSession) = {
    def createAuditView(viewSource: String) =
    {
      val appendString = if (viewSource == "table")
        s"$auditDB.audit where processname='$processName' and harddeletebatch != 'Y' and batchwindowend !='null' and loadstatus !='failedUnknown'"
      else
        s"auditView"

      val auditStr = spark.sql(s"select tableName , batch, ingestiondt, " +
        s"batchwindowstart, batchwindowend, harddeletebatch from $appendString")
        .coalesce(1)
      /**
        * auditStr : row(tableName , batch, ingestiondt, batchwindowstart, batchwindowend, harddeletebatch)
        *
        * FIX for EDIN-329 : Delta issue(Max batchendtime)
        */
      val audit = auditStr.withColumn("batchwindowstart", when(col("batchwindowstart").contains("-"), col("batchwindowstart"))
        .otherwise(lpad(col("batchwindowstart"),15,"0")))
        .withColumn("batchwindowend", when(col("batchwindowend").contains("-"), col("batchwindowend"))
          .otherwise(lpad(col("batchwindowend"),15,"0")))
        .groupBy(col("tableName"))
        .agg(max(col("ingestiondt")).as("ingestiondt"),
          max(col("batch")).as("batch"),
          ltrim(max(col("batchwindowstart")),"0").as("batchwindowstart"),
          ltrim(max(col("batchwindowend")),"0").as("batchwindowend")).cache

      audit.createOrReplaceTempView(s"${processName}_auditView")
      Holder.log.info("Entering the buildAuditData Function")
      val auditMapStr = audit.rdd.flatMap(row => Map(row.getAs[String](0)-> (row.getAs[String](1),
        row.getAs[String](2), row.getAs[String](3),row.getAs[String](4))))
        .collect
      val auditMap = auditMapStr.map(arrValue => arrValue._1 -> arrValue._2).toMap
      auditMap
    }

    if (spark.catalog.tableExists(s"${processName}_auditView"))
    createAuditView("table")
    else
    createAuditView("table")
  }

  def apply()(implicit spark: SparkSession) = {
    buildAuditData()
  }
}
