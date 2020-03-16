package edf.dataload

import edf.dataload._
import edf.dataload.auditutilities.{AuditLog, ConsolidateAuditEntry}
import edf.dataload.dfactions.{CreateDataFrame, LoadDataFrame, RefreshTypeLists}
import edf.dataload.helperutilities.{ReplicationTime, TableList}
import edf.recon.{gwRecon, gwclLakeQuery, gwclSourceQuery, gwplLakeQuery, gwplSourceQuery}
import edf.utilities.{Holder, MailingAgent, RichDF, TraversableOnceExt}
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

object DataIngestion extends SparkJob {
  implicit lazy val implicitConversions = scala.language.implicitConversions

  implicit def commomExtendTraversable[A, C[A] <: TraversableOnce[A]](coll: C[A]): TraversableOnceExt[C[A], A] =
    new TraversableOnceExt[C[A], A](coll, identity)

  implicit def commomExtendStringTraversable(string: String): TraversableOnceExt[String, Char] =
    new TraversableOnceExt[String, Char](string, implicitly)

  implicit def commomExtendArrayTraversable[A](array: Array[A]): TraversableOnceExt[Array[A], A] =
    new TraversableOnceExt[Array[A], A](array, implicitly)


  override def run(spark: SparkSession, propertyConfigs: Map[String, String]): Unit = {
    implicit val ss: SparkSession = spark
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.sparkContext.hadoopConfiguration.set("speculation", "false")

    spark.conf.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.sql.parquet.mergeSchema","false")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.hive.metastorePartitionPruning","true")
    spark.conf.set("spark.hadoop.parquet.enable.summary-metadata","false")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode",partitionOverwriteMode)
    spark.conf.set("spark.sql.broadcastTimeout","30000")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    if(stgLoadBatch) {
      spark.conf.set("spark.sql.thriftserver.scheduler.pool", "accounting")
      spark.conf.set("spark.scheduler.pool", "production")
    }
    val environment = getEnvironment(auditDB)
    val processnameInSubject = {
      if (hardDeleteBatch == "Y") s"${processName} with Hard Delete Batch"
      else processName
    }
    val batch_start_time = now.toString("YYYY-MM-dd HH:mm:ss.sss")
    val replicationTime = ReplicationTime()

    if (refTableListStr.trim != "")
      if((isConnectDatabase && !isTypeListToBeRefreshed) ||
        (restartabilityInd == "Y" && isConnectDatabase))
        spark.sql(s"select * from $auditDB.$oldConsolidateTypeList")
          .coalesce(1)
          .createOrReplaceTempView(oldConsolidateTypeList)
      else
      if(hardDeleteBatch == "N")
        RefreshTypeLists(batch_start_time,replicationTime)

   val auditResults =  TableList().par.
      map(table => {
        val lazydf = CreateDataFrame(table, 'N', batch_start_time)
        val loadDF = LoadDataFrame(lazydf._1, replicationTime, lazydf._2, lazydf._3)
        AuditLog(loadDF, batch_start_time, replicationTime)
      })
    val auditEntry = ConsolidateAuditEntry(auditResults.toList)
    Holder.log.info(auditEntry)
 MailingAgent.sendMail(s"$fromEmail", s"$toEmail",
        subject = s"${environment} : Ingestion notification from $processnameInSubject process",
   text = auditEntry)

   /* val gwclSourceQuery = s"""(SELECT
                             |    CAST('2020-03-05 23:59:59.999' AS DATE) as ExtractDate,
                             |    'Dummy' as PolStatus,
                             |    'TOTAL_INFORCE_'+ 'Dummy' as AuditEntity,
                             |    '1900-01-01' as AuditFrom,
                             |    '2020-03-05 23:59:59.999'  as AuditThru,
                             |    count(distinct polper.PolicyNumber) as AuditResult
                             |    from
                             |    dbo.pc_policyperiod polper) a""".stripMargin
    val gwclLakeQuery = s"""SELECT
                           |    CAST(cast('2020-03-05 23:59:59.999' as timestamp) AS DATE) as ExtractDate,
                           |    'Dummy' as PolStatus,
                           |    'TOTAL_INFORCE_Dummy' as AuditEntity,
                           |    '1900-01-01' as AuditFrom,
                           |    '2020-03-05 23:59:59.999'  as AuditThru,
                           |    count(distinct polper.PolicyNumber) as AuditResult
                           |    from
                           |    uat_l34_staging_gwcl.stg_gwcl_pc_policyperiod polper""".stripMargin
*/

    val reconResult =  processName match {
      case name if(name.startsWith("gwcl")) =>
                 //gwRecon(gwclSourceQuery, gwclLakeQuery, "GWCL")
                gwRecon(gwclSourceQuery, gwclLakeQuery, "GWCL")
      case name if(name.startsWith("gwpl")) =>
                  gwRecon(gwplSourceQuery, gwplLakeQuery, "GWPL")
      case _ => spark.emptyDataFrame
    }

    val htmlStr = RichDF(reconResult)
    MailingAgent.sendMail(s"$fromEmail", s"$toEmail",
      subject = s"${environment} : Ingestion notification from $processnameInSubject process",
      text = htmlStr)

    }

}
