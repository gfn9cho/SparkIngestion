package edf.dataload

import com.amazonaws.regions.Regions
import edf.dataload.auditutilities.{AuditLog, ConsolidateAuditEntry}
import edf.dataload.dfactions.{CreateDataFrame, LoadDataFrame, RefreshTypeLists}
import edf.dataload.helperutilities.{BackUpHiveDB, ReplicationTime, TableList}
import edf.recon._
import edf.utilities.{Holder, MailingAgent, RichDF, TraversableOnceExt}
import org.apache.spark.sql.{Row, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.immutable.Map
import scala.collection.parallel.ParSeq
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object SA360Ingestion extends SparkJob {
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

    if(stgLoadBatch || writeToDynamoDB) {
      spark.conf.set("spark.sql.thriftserver.scheduler.pool", "accounting")
      spark.conf.set("spark.scheduler.pool", "production")
    }

      val environment = getEnvironment(auditDB)
      val processnameInSubject = {
      if (hardDeleteBatch == "Y") s"${processName} with Hard Delete Batch"
      else processName
    }
     def batch_start_time = now.toString("YYYY-MM-dd HH:mm:ss.sss")
     val replicationTime = ReplicationTime()

      if (refTableListStr.trim != "")
        if((isConnectDatabase && !isTypeListToBeRefreshed) ||
          (restartabilityInd == "Y" && isConnectDatabase))
          spark.sql(s"select * from $auditDB.$oldConsolidateTypeList")
            .coalesce(1)
            .createOrReplaceTempView(oldConsolidateTypeList)
        else if(hardDeleteBatch == "N")
              RefreshTypeLists(batch_start_time,replicationTime)
    implicit val region = Regions.US_EAST_1

      //parallelTableList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(batchParallelism.toInt))
    while (now.getHourOfDay <= batchEndCutOffHour && now.getMinuteOfHour <= batchEndCutOffMinute) {
      val parallelTableList = TableList().par
      if(auditThroughStreaming) {
        val auditResults = parallelTableList.
          map(table => {
            val lazydf = CreateDataFrame(table, 'N', batch_start_time)
            val audit = LoadDataFrame(lazydf._1, replicationTime, lazydf._2, lazydf._3)
            //AuditLog(audit, batch_start_time, replicationTime, region)
            AuditLog(audit, batch_start_time, replicationTime, region)
          })
        val auditStats =  auditResults.map(stats => stats._1).toList
        val auditData = spark.createDataFrame(spark.sparkContext.parallelize(auditStats,1), schema)
        auditData.createOrReplaceTempView(s"${processName}_auditView")
      } else {
        val auditResults = parallelTableList.
          map(table => {
            val lazydf = CreateDataFrame(table, 'N', batch_start_time)
            val audit = LoadDataFrame(lazydf._1, replicationTime, lazydf._2, lazydf._3)
            //AuditLog(audit, batch_start_time, replicationTime, region)
            AuditLog(audit, batch_start_time, replicationTime)
          }).toList
       val auditData = Await.result(
          Future.sequence(auditResults), Duration.Inf
        ).toList
        ConsolidateAuditEntry(auditData)
      }

     /* val auditStats = auditResults.map(stats => stats._1).toList
      val auditData = spark.createDataFrame(spark.sparkContext.parallelize(auditStats,1), schema)
      auditData.createOrReplaceTempView(s"${processName}_auditView")*/
      import scala.util.control.Breaks._
      if(loadType == "TL" || loadType == "DI") System.exit(0)
    }
   }
}
