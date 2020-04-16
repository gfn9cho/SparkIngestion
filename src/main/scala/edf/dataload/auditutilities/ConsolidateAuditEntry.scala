package edf.dataload.auditutilities

import edf.dataload.{auditDB, auditPath, batchStatsSchema, datePartition,
      getEnvironment, processName, schema, sourceDBFormatted, splitString}
import edf.utilities.{GenerateHtmlContent, Holder, MailingAgent}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.immutable.Map

object ConsolidateAuditEntry {
  def closeActionsAndSendFailureMail(afterWriteResults: List[(Option[String], String, String, String, String, String)])
                                    (implicit spark: SparkSession)=
    afterWriteResults match {
      case batchTimes :: tail => {
        Holder.log.info("Inside the close Action Method")
        //BatchTimes is a Tuple Of(batchPartition, batchStartTime, Batch_window_start, Batch_Window_End, HardDeleteBatch)
        def batchStats = Seq(Row(processName, datePartition, batchTimes._5, batchTimes._4, batchTimes._2,
          batchTimes._3, batchTimes._6))
        val auditSaveMode = if(spark.catalog.tableExists(s"$auditDB.audit"))
          SaveMode.Append else SaveMode.Overwrite

        spark.createDataFrame(spark.sparkContext.parallelize(batchStats,1), batchStatsSchema)
          .write.format("parquet")
          .partitionBy("processname", "ingestiondt")
          .options(Map("path" -> (auditPath + "/batchstats")))
          .mode(auditSaveMode).saveAsTable(s"$auditDB.batchStats")

        val auditData = spark.sql(s"select * from $auditDB.audit where processname = '$processName' " +
          s"and ingestiondt = '$datePartition'").coalesce(1)

        auditData.write.format("parquet")
          .partitionBy("processname","ingestiondt")
          .options(Map("path" -> (auditPath + s"/$sourceDBFormatted/audit_temp")))
          .mode(SaveMode.Overwrite).saveAsTable(s"$auditDB.${sourceDBFormatted}_audit_temp")
        auditData.createOrReplaceTempView(s"${processName}_auditView")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
        spark.sql(s"select * from $auditDB.${sourceDBFormatted}_audit_temp where processname = '$processName' ")
          .write.format("parquet")
          .options(Map("path" -> (auditPath + "/audit"), "maxRecordsPerFile" -> "30000"))
          .mode(SaveMode.Overwrite).insertInto(s"$auditDB.audit")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","static")
        //spark.sql(s"alter table $auditDB.audit_temp drop if exists partition(processname='$processName')")
        spark.catalog.refreshTable(s"$auditDB.${sourceDBFormatted}_audit_temp")

        spark.catalog.refreshTable(s"$auditDB.audit")
        auditData.unpersist

        val header = List(schema.fieldNames.mkString(splitString))

        val data = header ::: batchTimes._1.toList ::: tail.flatMap(_._1.toList)

        //Send Mail only when there is a failure
        data match {
          case _ :: Nil => ""
          case _ =>
            Holder.log.info("Inside htmlContentStr")
            val environment = getEnvironment(auditDB)
            val htmlContentStr = GenerateHtmlContent.generateContent(data, processName, batchTimes._4, environment)
           htmlContentStr
        }
      }
      case _ => throw new Exception("No Data to Process. TableList is Empty")
    }
  def apply(afterWriteResults: List[(Option[String], String, String, String, String, String)])
           (implicit spark: SparkSession) = {
    closeActionsAndSendFailureMail(afterWriteResults)
  }
}
