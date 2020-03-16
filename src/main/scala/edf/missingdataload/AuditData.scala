package edf.missingdataload

import edf.utilities.Holder
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.immutable.Map

object AuditData {
  def buildAuditData(auditRowSeq: Seq[Row],
                     s3AuditPath: String, auditTable: String)(implicit spark: SparkSession): Unit = {
    val schema = StructType(
      List(
        StructField("processname", StringType, true),
        StructField("tablename", StringType, true),
        StructField("missingids", StringType, true),
        StructField("cutoffvalue", StringType, true),
        StructField("ingestiondt", StringType, true),
        StructField("batch", StringType, true),
        StructField("loadtime", StringType, true),
        StructField("insertInd", StringType, true),
        StructField("missingdatabatch", StringType, true)
      )
    )

    val auditData = spark.createDataFrame(
      spark.sparkContext.parallelize(
        auditRowSeq.filter(!_.getString(1).isEmpty), 1), schema)

    auditData.
      write.format("parquet").
      partitionBy("processname")
      .options(Map("path" -> s3AuditPath, "maxRecordsPerFile" -> "100000"))
      .mode(SaveMode.Append)
      .saveAsTable(auditTable)
  }

  def apply(auditRowSeq: Seq[Row],
            s3AuditPath: String, auditTable: String)(implicit spark: SparkSession) = {
    buildAuditData(auditRowSeq, s3AuditPath, auditTable)
  }
}
