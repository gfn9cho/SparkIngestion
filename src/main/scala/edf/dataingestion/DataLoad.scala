package edf.dataingestion

import java.sql.Timestamp

import edf.utilities._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Minutes}

import scala.collection.immutable.Map
import scala.concurrent._
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

object DataLoad extends SparkJob {

  implicit lazy val implicitConversions = scala.language.implicitConversions

  implicit def commomExtendTraversable[A, C[A] <: TraversableOnce[A]](coll: C[A]): TraversableOnceExt[C[A], A] =
    new TraversableOnceExt[C[A], A](coll, identity)

  implicit def commomExtendStringTraversable(string: String): TraversableOnceExt[String, Char] =
    new TraversableOnceExt[String, Char](string, implicitly)

  implicit def commomExtendArrayTraversable[A](array: Array[A]): TraversableOnceExt[Array[A], A] =
    new TraversableOnceExt[Array[A], A](array, implicitly)


  override def run(spark: SparkSession, propertyConfigs: Map[String, String]): Unit = {
    implicit val ss: SparkSession = spark
    //spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.sparkContext.hadoopConfiguration.set("speculation", "false")

    //spark.conf.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
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
    /**
      * Below function checks if there are any failures in DI / CI loads before triggering HD.
      * In case of any failures for any tables, it wont start Hard delete load.
      */

    def validateIngestionLoadBeforeHardDelete() : Boolean = { 
      val totalTablesFailureCount = spark.sql(s" select count(*) from  (select tablename,loadstatus,batch,ROW_NUMBER() " +
        s"OVER(PARTITION BY tablename ORDER BY batch DESC,loadendtime desc) AS r_num from $auditDB.audit " +
        s"where processname = '$processName' and harddeletebatch != 'Y') " +
        s"where r_num=1 and loadstatus != 'success' and batch in " +
        s"( select max(batch) from  $auditDB.audit where processname = '$processName' and harddeletebatch != 'Y') ").first().getAs[Long](0)
       Holder.log.info("Total tables failed in previous data ingestion : "+totalTablesFailureCount)
      (totalTablesFailureCount==0)
      }

    val environment = getEnvironment(auditDB)
    val processnameInSubject = {
      if (hardDeleteBatch == "Y") s"${processName} with Hard Delete Batch"
      else processName
    }
    def validationInDISuccess = validateIngestionLoadBeforeHardDelete

     if(hardDeleteBatch == "Y" && validationBeforeHDRequired=="true" && validationInDISuccess==false)
      {
          Holder.log.info(s"There seems to be some failures in latest ingestion batch ( CI / DI ).  " +
            s"please verify the same, rerun the ingestion and restart the hard delete batch")
          val htmlContentStr = s"There seems to be some failures in latest ingestion batch (CI / DI )" +
            s"please verify the same, rerun the ingestion and restart the hard delete batch \n"

          MailingAgent.sendMail(s"$fromEmail", s"$toEmail",
            subject = s"${environment} : Failure notification from $processnameInSubject process", text = htmlContentStr)
          spark.stop()
          System.exit(1)
          //spark.conf.set("spark.yarn.maxAppAttempts", 1)
          //throw new Exception("Failure in latest CI batch")
        }


val tableGroup: Map[String, List[String]] = tableInfos.map(info =>
(info._1.toString, info._2.toString + splitString + info._4.toString)).toList.toMultiMap

//val tableWithLookUp = tableGroup.keys.toList
//val pool = Executors.newFixedThreadPool(5)
//@transient implicit val xc = ExecutionContext.fromExecutorService(pool)

val dbConnectionAttempts = spark.sparkContext.longAccumulator("accumulator")
val jdbcSqlConnStr = JdbcConnectionUtility.constructJDBCConnStr(propertyConfigs)
val driver = JdbcConnectionUtility.getJDBCDriverName(propertyConfigs)
val mainTableList =  new MetaInfo(lookUpFile,tableFile).getTableSpecDetails.map(tables => tables.table)

val fullTableName = mainTableList.next

//val fullTableName = mainTableListFromTableSpec.head
Holder.log.info("###### The table name to connect to database " + fullTableName)




if(!(fullTableName.isEmpty) || !(fullTableName==null)) {
//TODO - Handles only sqlServer. need to handle other databases.
val dbConnectionStatusQuery = s"(select * from ${fullTableName} ) temp1"
Holder.log.info("The query to be executed for testing connectivity : " + dbConnectionStatusQuery)
def connectToDB(): Unit = {
try {
val result = spark.sqlContext.read.format("jdbc")
  .options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> dbConnectionStatusQuery)).load
Holder.log.info("The connection to database is successful, so continuing with further processing ")
}
catch {
case ex: Exception =>
  Holder.log.info("Exception while  connecting to database " + ex)
  Holder.log.info("Application thread is sleeping for 5 minutes and re-attempting to connect after 5 minutes ")
  dbConnectionAttempts.add(1)
  if (dbConnectionAttempts.value <= 3) {
    Thread.sleep(300000)
    connectToDB
  }
  else {
    Holder.log.info("The connection to database could not happen in last 15 minutes with multiple attempts, so stopping the processing.")
    val htmlContentStr = s"There was an exception while connecting to database ( for the table ${fullTableName} ). Please check the database connectivity."
    /**
      * Fix for EDIN-330 :  Any failure in harddelete batch getting email alert as dataExtract
      */
    MailingAgent.sendMail(s"$fromEmail", s"$toEmail",
      subject = s"${environment} : Failure notification from $processnameInSubject process", text = htmlContentStr)
    spark.stop()
    System.exit(1)
  }
}

}


val databaseConnectionStatus = connectToDB

}
else
{
Holder.log.info("There is no table present in table specs, so stopping the processing.")
val htmlContentStr = "There is no table present in table specs, the processing is stopped."
/**
* Fix for EDIN-330 :  Any failure in harddelete batch getting email alert as dataExtract
*/
MailingAgent.sendMail(s"$fromEmail", s"$toEmail",
subject = s"${environment} : Failure notification from $processnameInSubject process", text = htmlContentStr)
spark.stop()
System.exit(1)
}

/*
Get the list of tables failed in the previous batches
*/

val failedTableList = restartabilityInd match {
case "Y" => {
val failedTable = restartabilityLevel match {
  case "table" => spark.sql(s"select a.tableName, a.ingestiondt, a.batch, " +
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
    .rdd.map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4))).collect
  case "batch" => {
    val lastFailedBatch = spark.sql(s"select  max(batch) as batch from $auditDB.audit " +
      s"where processname = '$processName' ").first().getAs[String](0)

    spark.sql(s"select a.tableName, a.ingestiondt, a.batch, " +
      s"a.batchwindowstart, a.batchwindowend from $auditDB.audit a " +
      s"where batch='$lastFailedBatch' and batchwindowend != 'null'")
      .groupBy(col("tableName"))
      .agg(
        max(col("ingestiondt")).as("ingestiondt"),
        max(col("batch")).as("batch"),
        max(col("batchwindowstart")).as("batchwindowstart"),
        max(col("batchwindowend")).as("batchwindowend")
      )
      .rdd.map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4))).collect
  }
}
Some(failedTable)
}
case _ => None
}
def alterSchema(dftos3: Dataset[Row], hrmnzdHiveTableName: String, hiveSecureDB: String) = {
val sourceSchema = dftos3.drop("ingestiondt").schema.fields.map(field => field.name + " " + field.dataType.typeName)
val targetSchema = spark.sql(s"select * from $hrmnzdHiveTableName").drop("ingestiondt").schema.fields.map(field => field.name + " " + field.dataType.typeName)
val newFields = sourceSchema.map(_.toLowerCase) diff targetSchema.map(_.toLowerCase)
Holder.log.info("The new fields size to be added : " + newFields.size)
if(newFields.size > 0) {
val fieldsToAdd = sourceSchema.filter(field => newFields.map(_.toLowerCase).contains(field.toLowerCase)).mkString(",")
if (hiveSecureDB != "") {
  val hiveTableName = hrmnzdHiveTableName.split("\\.")(1)
  val secureHiveTableName = s"$hiveSecureDB.$hiveTableName"
  if (spark.catalog.tableExists(s"$secureHiveTableName")) {
    spark.sql(s"ALTER TABLE $secureHiveTableName set tblproperties('external'='false')")
    spark.sql(s"ALTER TABLE $secureHiveTableName ADD COLUMNS ($fieldsToAdd)")
    spark.sql(s"ALTER TABLE $secureHiveTableName set tblproperties('external'='true')")
  }
} else {
  spark.sql(s"ALTER TABLE $hrmnzdHiveTableName set tblproperties('external'='false')")
  spark.sql(s"ALTER TABLE $hrmnzdHiveTableName ADD COLUMNS ($fieldsToAdd)")
  spark.sql(s"ALTER TABLE $hrmnzdHiveTableName set tblproperties('external'='true')")
}
}
}

val saveMode = if (loadType == "TL" || restartabilityInd == "Y")
                            SaveMode.Overwrite else SaveMode.Append

def getBatchWindowStartTime = if (loadType == "TL") "1900-01-01"
else {
def batchStart = spark.sql(s"select max(batchwindowend) from $auditDB.batchStats where processname = '${processName}' and harddeletebatch != 'Y'")

batchStart.first().getAs[String](0)
}

def getReplTime: String = {
//val considerBatchWindow = propertyConfigs.getOrElse("spark.DataIngestion.considerBatchWindow", "")
//val dateNow = now.minusMillis(2)
//dateNow.add(Calendar.MILLISECOND, -2)
val replTimeNow = getUpdateTime(spark, propertyConfigs)
if (replTimeNow <= getBatchWindowStartTime && considerBatchWindowInd == "Y") {
  Thread.sleep(30000)
  getReplTime
}
replTimeNow
}

def buildAuditData = {
def createAuditView(viewSource: String) = {
val appendString = if (viewSource == "table")
  s"$auditDB.audit where processname='$processName' and harddeletebatch != 'Y' and batchwindowend !='null' and loadstatus !='failedUnknown'"
else
  s"auditView"

val auditStr = spark.sql(s"select tableName , batch, ingestiondt, " +
  s"batchwindowstart, batchwindowend, harddeletebatch from $appendString")
  .coalesce(1)

//Holder.log.info(auditStr.printSchema() +
/**
  * auditStr : row(tableName , batch, ingestiondt, batchwindowstart, batchwindowend, harddeletebatch)
  *
  * FIX for EDIN-329 : Delta issue(Max batchendtime)
  */
//val latestTLBatch = spark.sql(s"select max(batch) from $auditDB.audit where loadtype='TL' and processName = '${processName}' and harddeletebatch != 'Y' ").first().getAs[String](0)

val audit = auditStr.withColumn("batchwindowstart", when(col("batchwindowstart").contains("-"), col("batchwindowstart"))
  .otherwise(lpad(col("batchwindowstart"),15,"0")))
  .withColumn("batchwindowend", when(col("batchwindowend").contains("-"), col("batchwindowend"))
    .otherwise(lpad(col("batchwindowend"),15,"0")))
    .groupBy(col("tableName"))
  .agg(max(col("ingestiondt")).as("ingestiondt"),
  max(col("batch")).as("batch"),
    ltrim(max(col("batchwindowstart")),"0").as("batchwindowstart"),
    ltrim(max(col("batchwindowend")),"0").as("batchwindowend")).cache

/*        val audit = spark.sql(s"select tableName, max(ingestiondt) as ingestiondt, max(batch) as batch, " +
  s"max(batchwindowstart) as batchwindowstart, max(batchwindowend) as batchwindowend from $appendString  " +
  s"group by tableName").coalesce(1).cache()*/

audit.createOrReplaceTempView("auditView")
Holder.log.info("Entering the buildAuditData Function")
val auditMapStr = audit.rdd.flatMap(row => Map(row.getAs[String](0)-> (row.getAs[String](1),
                          row.getAs[String](2), row.getAs[String](3),row.getAs[String](4))))
                          .collect
//Holder.log.info("Built auditMapStr" + auditMapStr.size)
val auditMap = auditMapStr.map(arrValue => arrValue._1 -> arrValue._2).toMap
//spark.sparkContext.broadcast(auditMap)
//Holder.log.info("Inside the buildAuditData Function" + auditMap.size)
auditMap
}

if (spark.catalog.tableExists("auditView"))
// createAuditView("view")
createAuditView("table")
else
createAuditView("table")
}

/* Call createDF function with the list of tables to be ingested which inturn will
invoke the DataFrameLoader.readtable function to read the data from sqlServer, create the dataframe and
register as a TempTable
*/
def createDF(spark: SparkSession, tableList: String, propertyConfigs: Map[String, String],
           Indicator: Char, batchSize: Int = 100, batch_window_start: String ,
           replicationTime: String, hardDeleteBatch: String,
           auditMap: Option[Map[String,(String,String,String,String)]]): Iterator[Future[Try[String]]] = {
/* Below statement will initiate the batch window start and end time
 for every iteration of the batch window.
*/
val considerBatchWindow = if(hardDeleteBatch == "Y")
                                      "Y"
                                    else
                                      propertyConfigs.getOrElse("spark.DataIngestion.considerBatchWindow", "")

val current_window_Start: String = batch_window_start
val current_window_End: String =  replicationTime

def now = DateTime.now(timeZone)

val batch_start_time = now.toString("YYYY-MM-dd HH:mm:ss.sss")

val currentBatchList = (tableList, current_window_Start, current_window_End)

//Holder.log.info("TableList: " + tableList)
//Holder.log.info("TableList: " + failedTableList)

val dfFutures: Iterator[Future[Try[String]]] = failedTableList match {
  /*
* If the failed table list is not empty, check the restartability level and process only the table that are failed
* when the level is 'table', or process only the table that are not yet loaded when the table level is 'batch'
* */
  case Some(tables) if Indicator != 'Y' => {
    restartabilityLevel match {
      case "table" =>
        //Holder.log.info ("Inside MainTables with Indicator: " + Indicator)
        tables.map(list => {
          BatchConcurrency.executeAsync(DataFrameLoader.readTable(spark, list._1, propertyConfigs, Indicator,
            batch_start_time, list._4, list._5, list._3, hardDeleteBatch,
            tableSpecMap, refTableListfromTableSpec, cdcQueryMap))
        }).toIterator
      case "batch" =>
        val (start, end, part) = tables.toList match {
          case head :: _ => (head._4, head._5, head._3)
          case _ => (null, null, "")
        }
        val tablesNotYetLoaded = tableList.split(splitString,-1) diff tables.map(_._1)
        tablesNotYetLoaded.map(table => {
          val (batch_window_start, batch_window_end, batch_Partition) = considerBatchWindow match {
            case "Y" => (start, end, part)
            //case "N" if(restartabilityInd == "Y") => (start, end, part)
            case "N" =>
              if (loadType != "TL" && Indicator != 'Y') {
                val windowValues = auditMap match {
                  case Some(map) => map.getOrElse(s"$table", (null, null, null, null))
                  case None => (null, null, null, null)
                }
                val window_end_str = windowValues._4
                val window_end = if (window_end_str == null) "1900-01-01" else window_end_str
                (windowValues._3, window_end, part)
              } else (null, null,part)
          }
            BatchConcurrency.executeAsync(DataFrameLoader.readTable(spark, table.mkString, propertyConfigs, Indicator, batch_start_time,
              batch_window_start, batch_window_end, batch_Partition, hardDeleteBatch,
              tableSpecMap, refTableListfromTableSpec, cdcQueryMap))
        }).toIterator
    }
  }
  case None => {
    //builds a temp table by fetching the audit data

    currentBatchList._1
      .split(splitString,-1)
      .filter(_.trim != "")
      .filter(table => (hardDeleteBatch == "Y" &&
        tableSpecMap.getOrElse(table, "").split(splitString, -1)(1) == "id") || hardDeleteBatch != "Y"
      ) .map(table => {
        val (batch_window_start, batch_window_end) = considerBatchWindow match {
          case "Y" => (currentBatchList._2, currentBatchList._3)
          case "N" =>
            if (loadType != "TL" && Indicator != 'Y') {
              val windowValues = auditMap match {
                case Some(map) => map.getOrElse(s"$table",(null,null,null,null))
                case None =>  (null,null,null,null)
              }
              val window_end_str = windowValues._4
              val window_end = if (window_end_str == null) "1900-01-01" else window_end_str
              (windowValues._3, window_end)
              //(windowValues._3,"2019-08-20 16:08:58.546")
            } else (null, null)
        }
        BatchConcurrency.executeAsync(DataFrameLoader.readTable(spark, table.mkString, propertyConfigs, Indicator, batch_start_time,
          batch_window_start, batch_window_end, "", hardDeleteBatch,
          tableSpecMap, refTableListfromTableSpec, cdcQueryMap))
      }).toIterator
  }
}
dfFutures
}

def TypeListIngestionFutures(batchWindowStart: String, replicationTime: String) =
createDF(spark, refTableListStr, propertyConfigs, 'Y', 50, batchWindowStart,
                                          replicationTime,"N",None)


def writeTypeListTables(batchWindowStart: String, replicationTime: String) = {
  if(isConnectDatabase){
    if(isTypeListToBeRefreshed) {
      WriteTypeList.writeConnectTypeListTables(spark, propertyConfigs,
        TypeListIngestionFutures(batchWindowStart, replicationTime), mainTableListFromTableSpec)
    }
} else {
WriteTypeList.writeOtherTYpeListTables(spark, propertyConfigs,
  TypeListIngestionFutures(batchWindowStart, replicationTime), mainTableListFromTableSpec)
}
}

def processCompletedFutures(ingestionResult: (String, String), batchWindowStart: String,
                        replicationTime: String, hardDeleteBatch: String,
                        auditMap: Option[Map[String,(String,String,String,String)]]) = {
val writeFutures = {
val auditStr = ingestionResult._2.split(splitString)
val readStatistics = Row(auditStr(0), auditStr(1), auditStr(2), auditStr(3).toLong, auditStr(4), auditStr(5), auditStr(6), auditStr(7), auditStr(8), auditStr(9), auditStr(10), auditStr(11), auditStr(12))
//val tableToIngest = restartabilityInd match {case "Y" => auditStr(2) + "_" + auditStr(1) case _ => auditStr(2)}
val tableName = auditStr(2)
val cdcCol = getCdcColMax(tableName)
val batchPartition = auditStr(1)
val mainDF_arr = tableName.split("\\.")
val databaseName = if(mainDF_arr(0).contains("-"))
  mainDF_arr(0).replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_")
else
  mainDF_arr(0)
val mainDF = restartabilityInd match {
  case "Y" => databaseName + "_" + mainDF_arr(2) + "_" + batchPartition
  case "N" if hardDeleteBatch == "Y" => databaseName + "_" + mainDF_arr(2) + "_delete"
  case "N" => databaseName + "_" + mainDF_arr(2)
  }
//Get the previous window timing to log into audit incase of Failure
//For Success scenario, window timing will be based on current data
//FOr Failure scenario, capture previous window for restartability
val prevWindow = if (loadType != "TL") {
  val windowValues = auditMap match {
    case Some(map) => map.getOrElse(s"${auditStr(2)}",(null,null,null,null))
    case None =>  (null,null,null,null)
  }
  val window_end_str = windowValues._4
  val window_end = if (window_end_str == null) "1900-01-01" else window_end_str
  (windowValues._3, window_end)
} else if(cdcCol._2.endsWith("id") || cdcCol._2.endsWith("yyyymm") || cdcCol._2.startsWith("loadtime")) ("0","0")
else ("1900-01-01", "1900-01-01")

def emptyDataFrameInd = if (mainDF_arr(2).startsWith("dbo_")) false else
                            if(spark.catalog.tableExists(mainDF))
                                  Try {
                                    if(loadType == "TL")
                                      spark.table(mainDF).head(1).isEmpty
                                    else
                                    spark.table(mainDF).cache.head(1).isEmpty
                                  } match {
                                    case Success(bool) => bool
                                    case Failure(ex) =>
                                      throw new Exception(s"$mainDF failed with exception: ${ex.getLocalizedMessage}")
                                  }
                                  else false
val considerBatchWindow = if(hardDeleteBatch == "Y")
  "Y"
else
  propertyConfigs.getOrElse("spark.DataIngestion.considerBatchWindow", "")

def s3WriteIterator(acc: Int): Try[(Long, Long, String, String, Any, Any)] = {
  Try {
    writeDataFrametoS3(tableName, propertyConfigs, spark, metaInfoForLookupFile,
      tableGroup, saveMode, batchPartition, replicationTime, hardDeleteBatch)
  } match {
    case Success((srcCount, tgtCount, minWindow, maxWindow)) =>
      if (tgtCount == srcCount){
        //if consider batch window is ON, then use the data from dataframeLoader to capture the window timings.
        //Otherwise window timings will be calculated based on the updatetime minus 2 mins logic.
        val (minWindowStr, maxWindowStr) = considerBatchWindow match {
          case "Y" => (auditStr(8), auditStr(9))
          case _ => (minWindow, if(srcCount == 0L) prevWindow._2 else maxWindow)
        }
        Success((srcCount, tgtCount, "success", "",minWindowStr, maxWindowStr))
      }
        else
        acc match {
          case 0 => s3WriteIterator(1)
          case 1 =>
            val (minWindowStr, maxWindowStr) = considerBatchWindow match {
              case "Y" => (auditStr(8), auditStr(9))
              case _ => (minWindow, maxWindow)
            }
            Failure(new Throwable(srcCount + splitString + tgtCount + splitString + "failed"
            + splitString + "Count Mismatch" + splitString + minWindowStr + splitString + maxWindowStr))
        }
    case Failure(ex) =>
      acc match {
        case 0 => s3WriteIterator(1)
        case 1 =>
          val (minWindowStr, maxWindowStr) = considerBatchWindow match {
            case "Y" => (auditStr(8), auditStr(9))
            case _ => (prevWindow._1, prevWindow._2)
          }
          Failure(new Throwable(0L + splitString + 0L + splitString + "failedUnknown" +
          splitString + ex.getMessage + ex.getStackTrace.mkString("\n") + splitString + minWindowStr + splitString + maxWindowStr))
      }
  }
}

ingestionResult._1 match {
  case "success" =>
    val loadStartTime = now.toString("YYYY-MM-dd HH:mm:ss.sss")
 /*
 * EDIN-362: Create empty hive table in harmonized layer for TL
 */
    val loadStats =
      //if (!emptyDataFrameInd) s3WriteIterator(0) else Success((0L, 0L, "success", "",prevWindow._1,prevWindow._2))
      if (loadType == "TL" || stgLoadBatch || !emptyDataFrameInd ) s3WriteIterator(0) else Success((0L, 0L, "success", "",prevWindow._1,prevWindow._2))
    //dropping the temp view for the main table.

    if(spark.catalog.tableExists(mainDF)) {
      if(!refTableList.contains(tableName)) {
        spark.catalog.dropTempView(mainDF)
      }
    }
    loadStats match {
      case Success(stats) =>
        val minWindowStr = if(stats._5 == null) "" else stats._5.toString
        val maxWindowStr = if(stats._6 == null) "" else stats._6.toString
        val (srcCount, dfCount, loadStatus, ex, minWindow, maxWindow) = (stats._1, stats._2, stats._3, stats._4, minWindowStr,maxWindowStr)
        ("success", Row.fromSeq(readStatistics.toSeq :+
          dfCount :+
          hardDeleteBatch
          patch(3, Seq(srcCount), 1)
          patch(4, Seq(loadStartTime),1)
          patch(5, Seq(now.toString("YYYY-MM-dd HH:mm:ss.sss")),1)
          patch(6, Seq(loadStatus), 1)
          patch(7, Seq(ex), 1)
          patch(8, Seq(minWindow), 1)
          patch(9, Seq(maxWindow), 1)))
      case Failure(ex) =>
        val exStr = ex.getMessage.split(splitString)
        val (srcCount, dfCount, loadStatus, error, minWindow, maxWindow) = (exStr(0).toLong, exStr(1).toLong, exStr(2), exStr(3), exStr(4), exStr(5))
        val auditData = Row.fromSeq(readStatistics.toSeq :+
          dfCount :+
          hardDeleteBatch
          patch(3, Seq(srcCount), 1)
          patch(4, Seq(loadStartTime),1)
          patch(5, Seq(now.toString("YYYY-MM-dd HH:mm:ss.sss")),1)
          patch(6, Seq(loadStatus), 1)
          patch(7, Seq(error), 1)
          patch(8, Seq(minWindow), 1)
          patch(9, Seq(maxWindow), 1))
        ("failed", auditData)
    }
  case "failed" => ("failed", Row.fromSeq(readStatistics.toSeq :+ 0L :+ hardDeleteBatch))
}
}
writeFutures
}

def finalActionBasedOnWriteStatistics(writeStatistics: Iterator[Future[(String, Row)]],
                                  batchWindowStart: String, replUpdateTime: String) = {
BatchConcurrency.awaitSliding(writeStatistics, batchParallelism.toInt).map(stats => {

val auditFrame = spark.createDataFrame(spark.sparkContext.parallelize(Seq(stats._2),1), schema)
auditFrame
  .filter(!col("batchwindowend").isNull)
  .withColumn("processname", lit(processName))
  .write.format("parquet")
  .partitionBy("processname", "ingestiondt")
  .options(Map("path" -> (auditPath + "/audit")))
  .mode(SaveMode.Append).saveAsTable(s"$auditDB.audit")

val auditData = stats._1 match {
  case "failed" => Some(stats._2.mkString(splitString))
  case "success" => None
}

(auditData, batchWindowStart, replUpdateTime, stats._2.getAs[String](12), stats._2.getAs[String](1), stats._2.getAs[String](14))
}
)
}

def closeActionsAndSendFailureMail(afterWriteResults: List[(Option[String], String, String, String, String, String)]) =
  afterWriteResults match {
case batchTimes :: tail => {
Holder.log.info("Inside the close Action Method")
//BatchTimes is a Tuple Of(batchPartition, batchStartTime, Batch_window_start, Batch_Window_End, HardDeleteBatch)
def batchStats = Seq(Row(processName, datePartition, batchTimes._5, batchTimes._4, batchTimes._2,
                        batchTimes._3, batchTimes._6))

spark.createDataFrame(spark.sparkContext.parallelize(batchStats,1), batchStatsSchema)
  .write.format("parquet")
  .partitionBy("processname", "ingestiondt")
  .options(Map("path" -> (auditPath + "/batchstats")))
  .mode(SaveMode.Append).saveAsTable(s"$auditDB.batchstats")

val auditData = spark.sql(s"select * from $auditDB.audit where processname = '$processName' " +
  s"and ingestiondt = '$datePartition'").coalesce(1)

auditData.write.format("parquet")
  .partitionBy("processname","ingestiondt")
  .options(Map("path" -> (auditPath + s"/$sourceDBFormatted/audit_temp")))
  .mode(SaveMode.Overwrite).saveAsTable(s"$auditDB.${sourceDBFormatted}_audit_temp")

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
  case _ :: Nil => None
  case _ =>
    Holder.log.info("Inside htmlContentStr")
    val environment = getEnvironment(auditDB)
    val processnameInSubject = {
      if (hardDeleteBatch == "Y") s"${processName} with Hard Delete Batch"
      else processName
    }
    val htmlContentStr = GenerateHtmlContent.generateContent(data, processName, batchTimes._4, environment)
    MailingAgent.sendMail(s"$fromEmail", s"$toEmail",
      subject = s"${environment} : Failure notification from $processnameInSubject process", text = htmlContentStr)
}
}
case _ => Holder.log.info("empty seq")
}

def tableIngestionFutures(batchWindowStart: String, replicationTime: String, hardDeleteBatch: String,
           auditMap: Option[Map[String,(String,String,String,String)]]): Iterator[Future[Try[String]]] = {
val tableList = if(hardDeleteBatch == "Y")
                    deleteTableList.mkString(splitString)
                  else if(stgLoadBatch) stgTableList.mkString(splitString)
                      else tableIngestionList.sortBy(_.split("\\.")(1)).mkString(splitString)

createDF(spark, tableList, propertyConfigs, 'N', batchParallelism.toInt, batchWindowStart,
replicationTime, hardDeleteBatch, auditMap)
}


def statistics(batchWindowStart: String, replicationTime: String, hardDeleteBatch: String,
           auditMap: Option[Map[String,(String,String,String,String)]]) =
BatchConcurrency.awaitSliding(tableIngestionFutures(batchWindowStart, replicationTime, hardDeleteBatch, auditMap),

  batchParallelism.toInt).map {
case Success(value) => {
("success", value)
}
case Failure(exception) => {
("failed", exception.getMessage)
}
}.filter(!_._2.split(splitString).contains(".cdc."))
.map(x => BatchConcurrency.executeAsync(processCompletedFutures(x, batchWindowStart,replicationTime, hardDeleteBatch, auditMap)))

/*def iterAction[A](iterator: Iterator[A]): List[A] = {
def consolidate(iterator: Iterator[A], list: List[A]): List[A] = {
iterator.hasNext match {
  case true => consolidate(iterator, List(iterator.next) ::: list)
  case false => list
}
}
consolidate(iterator, List.empty)
}*/

if (loadType == "TL" || loadType == "DI") {
val batchWindowStart = getBatchWindowStartTime
val replicationTime = getReplTime
if (refTableListStr.trim != "")
if((isConnectDatabase && !isTypeListToBeRefreshed) ||
  (restartabilityInd == "Y" && isConnectDatabase))
  spark.sql(s"select * from $auditDB.$oldConsolidateTypeList")
    .coalesce(1)
    .createOrReplaceTempView(oldConsolidateTypeList)
else
  if(hardDeleteBatch == "N")
    writeTypeListTables(batchWindowStart,replicationTime)

if(stgLoadBatch) {
  spark.sql(
    s"select max(batch) as batch from $auditDB.audit " +
      s"where processname = '$extractProcessName' and HardDeleteBatch = 'Y' ")
    .cache()
    .createOrReplaceTempView("hardDeleteBatch")
}
val auditMap = if(loadType == "DI" && hardDeleteBatch == "N") Some(buildAuditData) else None
val afterWriteResults = finalActionBasedOnWriteStatistics(
  statistics(batchWindowStart, replicationTime,hardDeleteBatch, auditMap),
  batchWindowStart, replicationTime)
val resultConsolidation = iterAction(afterWriteResults)
closeActionsAndSendFailureMail(resultConsolidation)
}
else if(loadType == "RB") {
  writeToS3(spark.emptyDataFrame, s3Location,"",SaveMode.Append,"","")
  writeToS3(spark.emptyDataFrame, s3SecuredLocation ,"",SaveMode.Append,"","")
}
else {
if (refTableListStr.trim != "")
  if((isConnectDatabase && !isTypeListToBeRefreshed) ||
    (restartabilityInd == "Y" && isConnectDatabase))
  spark.sql(s"select * from $auditDB.$oldConsolidateTypeList")
    .coalesce(1)
    .createOrReplaceTempView(oldConsolidateTypeList)
else
if(hardDeleteBatch == "N")
  writeTypeListTables(getBatchWindowStartTime, getReplTime)
while (now.getHourOfDay <= batchEndCutOffHour && now.getMinuteOfHour <= batchEndCutOffMinute) {
val auditMap = Some(buildAuditData)
Holder.log.info("Next Batch Started")
val timeZone = DateTimeZone.forID("America/New_York")
def dateInstance = DateTime.now(timeZone)
val beforeBatchIngestion = dateInstance.millisOfDay().get
val batchWindowStart = getBatchWindowStartTime
val replicationTime = getUpdateTime(spark, propertyConfigs)

Holder.log.info("hardDeleteBatch: " + hardDeleteBatch)

val afterWriteResults = finalActionBasedOnWriteStatistics(
  statistics(batchWindowStart, replicationTime, hardDeleteBatch,auditMap),
  batchWindowStart, replicationTime)
val resultConsolidation = iterAction(afterWriteResults)
closeActionsAndSendFailureMail(resultConsolidation)
//deleteStagePartition -- not used as staging process has been moved out of data ingestion process
Holder.log.info("Batch Completed")
val afterBatchIngestion = dateInstance.millisOfDay().get
val batchTimeTaken = afterBatchIngestion - beforeBatchIngestion
if (batchTimeTaken < 300000) Thread.sleep(300000 - batchTimeTaken)
}
}

def writeDataFrametoS3(tableToBeIngested: String, propertyConfigs: Map[String, String],
                   spark: SparkSession, metaInfoLookUp: MetaInfo, tableGroup: Map[String, List[String]],
                   saveMode: SaveMode, batchPartition: String, replicationTime: String,
                   hardDeleteBatch: String) = {
  val ref_col_list = metaInfoLookUp.getRefColList
  val tableDF_arr = tableToBeIngested.split("\\.")
  val formattedDBName = formatDBName(tableDF_arr(0))
  val tableDF = restartabilityInd match {
                    case "Y" => formattedDBName + "_" + tableDF_arr(2) + "_" + batchPartition
                    case _ => formattedDBName + "_" + tableDF_arr(2) }

  val piiColList = piiList.toMultiMap.getOrElse(tableToBeIngested, List.empty[String])
  val mainDF_arr = tableToBeIngested.split("\\.")
  val tableKey = if(tableDF.endsWith("_CT") && tableDF.contains("_dbo_"))
                    mainDF_arr(0) + ".cdc.dbo_" + mainDF_arr(2) + "_CT"
                 else
                    tableToBeIngested
  val cdcColMaxStr = getCdcColMax(tableKey)
  val considerBatchWindow = if(hardDeleteBatch == "Y") "Y" else considerBatchWindowInd
  val deleteString = if(hardDeleteBatch == "Y") "_delete" else ""
  val (cdcColMax, min_window, max_window) = if(considerBatchWindow == "Y" ||
                                                (stgLoadBatch && loadType == "TL" &&
                                                  !cdcColMaxStr._2.endsWith("id")))
                                                  (null,null,null)
                                            else
                                              getMinMaxCdc(tableDF, tableKey, replicationTime)
  val partitionByCol = cdcColMaxStr._3
  val bucketColumn = if(cdcColMaxStr._2.endsWith("id") ||
                        cdcColMaxStr._2.endsWith("yyyymm") ||
                          cdcColMaxStr._2.startsWith("loadtime"))
                      col("ingestiondt")
                    else
                      cdcColMaxStr._1

  val dfBeforePii = hardDeleteBatch match {
    case "Y" => spark.sql(s"select * from $tableDF$deleteString")
      .withColumn("ingestiondt", trunc(date_format(
        bucketColumn, "YYYY-MM-dd"), "MM"))
      .withColumn("uniqueId", concat(bucketColumn.cast("Long"),
        col(partitionByCol)))
    case "N"
      if stgLoadBatch && loadType == "TL" => getTLDataFromHrmnzd(tableDF_arr(2), piiColList, cdcColMaxStr._2)
    case "N" => joinTypeTables(spark, tableToBeIngested, ref_col_list,
                              tableGroup, batchPartition).filter(cdcColMax <= max_window)
  }
  val (srcCount, tgtCount) = handlePiiData(dfBeforePii, piiColList, tableToBeIngested,
    batchPartition, cdcColMaxStr._2, saveMode)
  if(stgLoadBatch && loadType == "TL" && !cdcColMaxStr._2.endsWith("id")) {
    val window = dfBeforePii.agg(min(cdcColMaxStr._1).as("min_window"),
      max(cdcColMaxStr._1).as("max_window")).rdd.
      map(r => (r.getTimestamp(0), r.getTimestamp(1))).first()
    (srcCount, tgtCount, window._1, window._2)
  }
  else
  (srcCount, tgtCount, min_window, max_window)
}

def joinTypeTables(spark: SparkSession, mainTable: String, ref_col_list: List[String],
               tableGroup: Map[String, List[String]], batchPartition: String) = {
  val typeTableList: List[String] = tableGroup.get(mainTable) match {
                                        case Some(tables: List[String]) => tables
                                        case None => Nil
                                       }
  val mainDF_arr = mainTable.split("\\.")
  val formattedSourceDBName = if(mainDF_arr(0).contains("-"))
                                mainDF_arr(0).
                                  replaceAll("\\[", "").
                                    replaceAll("\\]", "").
                                      replaceAll("-", "_")
                              else
                                mainDF_arr(0)
  val mainDFStr = restartabilityInd match {
                      case "Y" => formattedSourceDBName + "_" + mainDF_arr(2) + "_" + batchPartition
                      case "N" => formattedSourceDBName + "_" + mainDF_arr(2)
                      }

  val tableSpecMapStr = tableSpecMap.getOrElse(mainTable, "").split(splitString, -1)
  val hardDeleteFlag = tableSpecMapStr(1)
  val cdcCol = getCdcColMax(mainTable)
  val bucketColumn = if(cdcCol._2.endsWith("id") ||
                          cdcCol._2.endsWith("yyyymm") ||
                            cdcCol._2.startsWith("loadtime"))
                        col("ingestiondt")
                      else
                        cdcCol._1
  val partitionByCol = col(cdcCol._3)
  val cdcDF = mainDF_arr(0) + "_dbo_" + mainDF_arr(2) + "_CT"
  val mainDFBeforeRepartition = spark.sql(s"select * from $mainDFStr")
                                  .withColumn("ingestiondt", date_format(
                                        bucketColumn, "YYYY-MM-dd"))
                                  .withColumn("uniqueId", concat(
                                        bucketColumn.cast("Long"),partitionByCol))
  val numPart = if(loadType == "TL")
                    mainDFBeforeRepartition.
                        select(trunc(col("ingestiondt"),"MM").as("months")).
                        dropDuplicates.count.toInt * 2
                else
                    incrementalpartition.toInt
//Holder.log.info("NumPartitions: " + numPart + "-" + bucketColumn)
/*
 * EDIN-362: Create empty hive table in harmonized layer for TL
 */
  val mainDF = if(numPart > 0)
                mainDFBeforeRepartition
                  .repartition(numPart,col("ingestiondt"))
                  .withColumn("ingestiondt", trunc(date_format(
                      bucketColumn, "YYYY-MM-dd"),"MM").cast("String"))
               else
                mainDFBeforeRepartition
                  .withColumn("ingestiondt", trunc(date_format(
                      bucketColumn, "YYYY-MM-dd"),"MM").cast("String"))
  val srcDF = if (hardDeleteFlag == "cdc")
                mainDF
                  .withColumn("deleted_flag", lit(0))
                  .union(spark.sql(s"select * from $cdcDF")
                  .withColumn("deleted_flag", lit(1)))
              else if(hardDeleteFlag == "id")
                      mainDF.withColumn("deleted_flag", lit(0))
                    else
                      mainDF

def makeJoins(df: DataFrame, typeTableList: List[(String, Int)]): DataFrame = {
typeTableList match {
  case Nil => df
  case types :: tail => {
    val typeTableSqlSplit = types._1.split(splitString, -1)
    val typeTable_arr = typeTableSqlSplit(0).split("\\.")
    val formattedTypeDBName = if(typeTable_arr(0).contains("-"))
      typeTable_arr(0).replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("-", "_")
      else
      typeTable_arr(0)
    val typeTable = formattedTypeDBName + "_" + typeTable_arr(2)
    val sqlStr = typeTableSqlSplit(1)
    val typeDF = if(isConnectDatabase) {
      val typeData = spark.table(oldConsolidateTypeList).
        filter(col("typelistName") === typeTableSqlSplit(0))
      if(!typeData.head(1).isEmpty)
        typeData
      else
        spark.sql(s"select * from $targetDB.${typeTable_arr(2)}")
    } else
      spark.sql(s"select * from $typeTable")
    val condition = SqlConditionBuilder.parseCondition(types._2, sqlStr, ref_col_list)
    val selfJoinInd = if (mainDF == typeTable) 'Y' else 'N'
    val joinedDF = joinDFs(df, typeDF, condition, "left", selfJoinInd)

    makeJoins(joinedDF, tail)
  }
}
}

makeJoins(srcDF, typeTableList.zipWithIndex)
}

def joinDFs(dfL: DataFrame, dfR: DataFrame, conditions: (String, List[String]), joinType: String, selfJoinInd: Char) = {

val (sCondition_full, jCondition_full) = (conditions._1.split(","), conditions._2)
val colAliasPrefix = if (selfJoinInd == 'Y') "dfr_" else ""
val sCondition = sCondition_full.map(_.replace(".", "").split("-") match {
case Array(x: String, y: String) => {
  //Holder.log.info("####Capturing select fields inside parseCondition: " + x + ":" + y)
  col(`x`).as(y)
}
case Array(x: String) => {
   if (selfJoinInd == 'Y') col(`x`).as(colAliasPrefix + x) else col(x)
}
})

val jCondition = jCondition_full.map(cond => {
val condType = cond.split("-")
condType(0) match {
  case "jSeq" => col(condType(1)) === col(colAliasPrefix + condType(1))
  case "jEqui" => {
    val condSplit = condType(1).split(":")
    col(condSplit(0)) === col(colAliasPrefix + condSplit(1))
  } case "wEqui" => {
    val condSplit = condType(1).split(":")
    col(colAliasPrefix + condSplit(0)) === condSplit(1)
  }
  case "wNull" => col(colAliasPrefix + condType(1)).isNull
}
}
).reduce(_ and _)

val dPattern = """jEqui-.*|wEqui-.*|wNull-.*""".r
val dCondition: List[String] = jCondition_full.map(cond => {
dPattern.findFirstIn(cond) match {
  case Some(clmn: String) => {
    val replacePattern = """jEqui-.*:|wEqui-|wNull-""".r
    val dropclmn = replacePattern.replaceFirstIn(clmn, "")
    //if(selfJoinInd == 'Y') "dfr_" + dropclmn else dropclmn
    colAliasPrefix + dropclmn.split(":")(0)
  }
  case None => ""
}
}
)
dfL.join(broadcast(dfR.select(sCondition: _*).dropDuplicates), jCondition, joinType).drop(dCondition: _*)
}
}

def getUpdateTime(spark: SparkSession, propertyConfigs: Map[String, String]): String = {
  val sourceDB = propertyConfigs.getOrElse("spark.DataIngestion.sourceDB", "").
                        replaceAll("\\[","").
                            replaceAll("\\]","").
                              replaceAll("-","_")
  val repl_table = propertyConfigs.getOrElse("spark.DataIngestion.repl_table", "")
  val timeLagInMins = propertyConfigs.getOrElse("spark.DataIngestion.timeLagInMins", "")
  val jdbcSqlConnStr = JdbcConnectionUtility.constructJDBCConnStr(propertyConfigs)
  val driver = JdbcConnectionUtility.getJDBCDriverName(propertyConfigs)

//TODO - Handles only sqlServer. need to handle other databases.
  val repl_query = if (repl_table.trim != "")
                      s"(select max(dateadd(mi, $timeLagInMins, repl_updatetime)) UpdateTime " +
                      s"from $sourceDB.dbo.$repl_table) UpdateTime"
                   else
                      s"(select dateadd(mi, $timeLagInMins, getdate()) as UpdateTime) UpdateTime "

  def UpdateTime = Try {
      spark.sqlContext.read.format("jdbc")
                      .options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver,
                                    "dbTable" -> repl_query)).load.first
                        }
  UpdateTime match {
    case Success(row) => row.getAs[Timestamp](0).toString
    case Failure(exception) => throw exception
  }
}
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

  def getMinMaxCdc(tableDF: String, tableKey: String,
                   replicationTime: String)(implicit spark: SparkSession): (Column, Any, Any) = {
    val cdcColMaxStr = getCdcColMax(tableKey)
    if (cdcColMaxStr._2.endsWith("id") || cdcColMaxStr._2.endsWith("yyyymm") || cdcColMaxStr._2.endsWith("loadtime")) {
      val dateStrDF = spark.table(tableDF)
        .select(cdcColMaxStr._1.as("max_window_end"))
        .agg(min(col("max_window_end")), max(col("max_window_end")))
      val dateStr = dateStrDF.first()
      (cdcColMaxStr._1, dateStr.getAs[Long](0), dateStr.getAs[Long](1))
    } else {
      val coalesceDateHaving9999 = cdcColMaxStr._4
      val dateStrDF =
        if(coalesceDateHaving9999 == null)
          spark.table(tableDF)
            .select(cdcColMaxStr._1.as("max_window_end"))
            .agg(min(col("max_window_end")), max(col("max_window_end")))
        else
          spark.table(tableDF)
            .withColumn(coalesceDateHaving9999,
              when(col(coalesceDateHaving9999).startsWith("9999"),lit(null))
                .otherwise(col(coalesceDateHaving9999)))
            .select(cdcColMaxStr._1.as("max_window_end"))
            .agg(min(col("max_window_end")), max(col("max_window_end")))

      val dateStr = dateStrDF.first()
      Holder.log.info("dateStr: " + dateStr.mkString(",") + ":" + cdcColMaxStr.toString())
      val minDate = dateStr.getAs[Timestamp](0)
      val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")
      val currentReplTime = datetime_format.withZone(timeZone).parseDateTime(replicationTime)
      val currentReplTimeTruncated = currentReplTime.withTimeAtStartOfDay().minusMillis(1)
      val maxDate = dateStr.getAs[Timestamp](1) match {
        case date: Timestamp =>
          val maxDate = datetime_format.withZone(timeZone).parseDateTime(date.toString)
          Holder.log.info("maxDate: " + maxDate)
          val timeLagInUpdates =   Minutes.minutesBetween(currentReplTime, maxDate).getMinutes

          val minutesBetweenMinAndMaxDates = Minutes.minutesBetween(maxDate,
            datetime_format.withZone(timeZone).parseDateTime(minDate.toString)).getMinutes
          val maxDateMinus2Mins = if(timeLagInUpdates > 30 ||
            minutesBetweenMinAndMaxDates == 0) maxDate else maxDate.minusMinutes(timeLagInMins)
          val cutOffMaxDate = stgLoadBatch match {
            case true => if (maxDateMinus2Mins.getMillis > currentReplTimeTruncated.getMillis)
              currentReplTimeTruncated else  maxDateMinus2Mins
            case false => maxDateMinus2Mins
          }
          Some(cutOffMaxDate.toString("YYYY-MM-dd HH:mm:ss.SSS"))
          //Some("2019-08-01 16:08:58.546")
        case _ => None
      }

      (cdcColMaxStr._1,minDate, maxDate.orNull)
    }
}

def handlePiiData(df: DataFrame, piiColList: List[String], tableToBeIngested: String,
                  batchPartition: String, cdcColMax: String, saveMode: SaveMode)
                 (implicit spark: SparkSession) = {
  val tableDF_arr = tableToBeIngested.split("\\.")
  val hiveTableName = if(stgLoadBatch)
                          s"$hiveDB.$stageTablePrefix" + tableDF_arr(2)
                      else
                          s"$hiveDB." + tableDF_arr(2)
  val hiveSecureTable = if(stgLoadBatch)
                          s"$hiveSecuredDB.$stageTablePrefix" + tableDF_arr(2)
                        else
                          s"$hiveSecuredDB." + tableDF_arr(2)

  def piiDataClassification(df: DataFrame, piiColList: List[String]) : (Long, Long) = {
    piiColList match {
      case pii: List[String]
        if pii.isEmpty =>
        Holder.log.info("hiveDB: " + hiveDB + "-" + hiveTableName)
        writeToS3(df, s3Location + tableDF_arr(2),
        hiveTableName, saveMode, batchPartition, cdcColMax)
      case pii: List[String] => {
        Holder.log.info("hiveDBSecure: " + hiveSecuredDB + "-" + hiveSecureTable)
        writeToS3(df, s3SecuredLocation + tableDF_arr(2),
          hiveSecureTable, saveMode, batchPartition, cdcColMax)
        val dfFromS3 = spark.sql(s"select * from $hiveSecureTable where " +
          s"batch = '$batchPartition'")
        val dfUpdated: DataFrame = pii.foldLeft(dfFromS3)((d, c) => d.withColumn(c, lit("")))
        piiDataClassification(dfUpdated, List.empty[String])
      }
    }
  }
  piiDataClassification(df, piiColList)
  }

  def getTLDataFromHrmnzd(tableName: String, piiColList: List[String], cdcCol: String)
                         (implicit spark: SparkSession) = {
    val hrmnzdDB = piiColList match {
      case pii: List[String]
        if pii.isEmpty => loadFromStage match {
        case true =>  propertyMap.getOrElse ("spark.DataIngestion.targetStageDB", "")
        case false => propertyMap.getOrElse ("spark.DataIngestion.targetDB", "")
      }
      case _: List[String] => loadFromStage match {
        case true => propertyMap.getOrElse("spark.DataIngestion.targetSecuredStageDB", "")
        case false => propertyMap.getOrElse("spark.DataIngestion.targetSecuredDB", "")
      }
    }

    def maxTLBatch = spark.sql(s"select max(batch) from $auditDB.audit " +
      s"where processname = '$extractProcessName' and loadType = 'TL' and " +
      s"tablename like '%$tableName' ").first().getString(0)

    val df = if (loadFromStage) {
      //spark.sql(s"select * from ${hrmnzdDB.replaceAll("test_","")}.$stageTablePrefix$tableName")
      spark.sql(s"select * from edf_staging.$stageTablePrefix$tableName")

    }
      else if (loadOnlyTLBatch) {
      spark.sql(s"select * from $hrmnzdDB.$tableName where " +
        s"batch = '$maxTLBatch' ")
    } else {
      val fullData = spark.sql(s"select * from $hrmnzdDB.$tableName")
      val sortCols = if (fullData.schema.fieldNames.contains("deleted_flag"))
                        List("uniqueid") else List(cdcCol)
      if(cdcCol.endsWith("id"))
        fullData
      else
        getMostRecentRecords(fullData, List("id"), List("uniqueid", "batch"))
    }

    df
  }
  def getMostRecentRecords(df: DataFrame, partition_col: List[String],
                    sortCols: List[String]): DataFrame = {
    val part = Window.
                  partitionBy(partition_col.head,partition_col:_*).
                  orderBy(array(sortCols.head,sortCols:_*).desc)
    val dfWithRank = df.withColumn("rn", row_number().over(part))
    val dfWithDuplicatesRemoved = dfWithRank.
                                      filter("rn==1").
                                      drop("rn")
    dfWithDuplicatesRemoved
  }
}

