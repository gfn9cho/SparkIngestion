package edf.dataload

import edf.dataload._
import edf.dataload.auditutilities.{AuditLog, ConsolidateAuditEntry}
import edf.dataload.dfactions.{CreateDataFrame, LoadDataFrame, RefreshTypeLists}
import edf.dataload.helperutilities.{BackUpHiveDB, ReplicationTime, TableList}
import edf.recon.{gwRecon, gwclLakeQuery, gwclSourceQuery, gwplLakeQuery, gwplSourceQuery}
import edf.utilities.{Holder, MailingAgent, RichDF, TraversableOnceExt}
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

/**
  * Main class to invoke inorder to perform the ingestion/staging load.
  *   =a. Stage or ingestion load.=
  *         if stageLoadBatch is set to true,<br />
  *         Read the staging list file and perform the staging load,<br />
  *         Otherwise table in the table spec file will be read and perform the ingestion load.
  *   =b. Refresh Typelist?=
  *         Tyeplist will be be read from source and refreshed in the harmonized
  *         along with the consolidated type list in the following case<br />
  *           1. If the source db is connect and is typelist refresh is enabled.<br />
  *           2. If the source db is connect and when the job is restarted.<br />
  *          otherwise existing values from the consolidated typelist will be used.<br />
  *  = c. Process tables=
  *         For each table the following actions will be perfromed,<br />
  *         1. Create DataFrame(lazy) to read the incremental data from the source using [[edf.dataload.dfactions.CreateDataFrame]]<br />
  *         2. Load the dataframe and apply transformations like typelist joins, calculating cdc max values,<br />
  *            writing to s3 and generating audit data etc. <br />
  *         3. Log audit entry for the table processed. <br />
  *   = d. Final actions= <br />
  *         Once all the tables has been processed, audit logs will be consolidated and rewritten <br />
  *         back to the audit table as one single parquet file. <br />
  *         An email will be sent in case of any failure with the details.<br />
  *         In case of staging load, reconciliation process will also be performed <br />
  *         and a email will be sent with the recon metrics.
  */

object DataIngestion extends SparkJob {
  implicit lazy val implicitConversions = scala.language.implicitConversions

  implicit def commomExtendTraversable[A, C[A] <: TraversableOnce[A]](coll: C[A]): TraversableOnceExt[C[A], A] =
    new TraversableOnceExt[C[A], A](coll, identity)

  implicit def commomExtendStringTraversable(string: String): TraversableOnceExt[String, Char] =
    new TraversableOnceExt[String, Char](string, implicitly)

  implicit def commomExtendArrayTraversable[A](array: Array[A]): TraversableOnceExt[Array[A], A] =
    new TraversableOnceExt[Array[A], A](array, implicitly)

  /**
    * Method implemented from SparkJob trait, serves as a entry point for the application.
    *
    * @param spark -spark Session initiated in the trait.
    * @param propertyConfigs Map of property name to value as specified in the property configs.
    * @version 1.0
    * @see See [[https://danielwestheide.com/books/the-neophytes-guide-to-scala/]] for scala tutorial.
    * @note
    *       stgLoadBatch determines whether its a staqing or ingestion load. <br />
    *       backUpStageforTL is used to take a backup of the existing staging db for TL load. <br />
    *       Recon jobs will run only for staging load.
    * @todo CI load is yet to be implemented.
    */
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
    if(stgLoadBatch && loadType == "TL" && loadFromStage && backUpStageforTL) {
      BackUpHiveDB(stgTableList, backupFromDB, backupFromSecuredDB,
        initialLoadStagingDB, backUpToLocation)
    } else {
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
        else if(hardDeleteBatch == "N")
              RefreshTypeLists(batch_start_time,replicationTime)

      val auditResults =  TableList().par.
        map(table => {
          val lazydf = CreateDataFrame(table, 'N', batch_start_time)
          val loadDF = LoadDataFrame(lazydf._1, replicationTime, lazydf._2, lazydf._3)
          AuditLog(loadDF, batch_start_time, replicationTime)
        })
      val auditEntry = ConsolidateAuditEntry(auditResults.toList)
      MailingAgent.sendMail(s"$fromEmail", s"$toEmail",
        subject = s"${environment} : Ingestion notification from $processnameInSubject process",
        text = auditEntry)

      if(stgLoadBatch) reconResult(processnameInSubject, environment)
    }
    }

  /**
    * @param processnameInSubject - Specify the processName in the email subject.
    * @param environment - Used to qualify the email subject with the environment..
    * @note Based on the source system code in the processName, <br />
    *       respective recon queries will be fetched from the package object and <br />
    *       recon jobs will be run to collect metrics. <br />
    *       Once the Recon Job is finished, an email will be triggered with the recon metrics.
    */

  def reconResult(processnameInSubject: String, environment: String)(implicit spark: SparkSession) = {
    val reconResult = processName match {
      case name if (name.startsWith("gwcl")) =>
        //gwRecon(gwclSourceQuery, gwclLakeQuery, "GWCL")
        gwRecon(gwclSourceQuery, gwclLakeQuery, "GWCL")
      case name if (name.startsWith("gwpl")) =>
        gwRecon(gwplSourceQuery, gwplLakeQuery, "GWPL")
      case _ => spark.emptyDataFrame
    }
    val htmlStr = RichDF(reconResult)
    MailingAgent.sendMail(s"$fromEmail", s"$toEmail",
      subject = s"${environment} : Ingestion notification from $processnameInSubject process",
      text = htmlStr)
  }
}
