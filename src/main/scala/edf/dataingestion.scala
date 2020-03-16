package edf
import edf.dataingestion.CdcQueryBuilder
import edf.utilities.{JdbcConnectionUtility, MetaInfo, TraversableOnceExt, sqlQueryParserFromCSV}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.immutable.Map
import scala.io.Source

package object dataingestion {

  implicit lazy val implicitConversions = scala.language.implicitConversions

  implicit def commomExtendTraversable[A, C[A] <: TraversableOnce[A]](coll: C[A]): TraversableOnceExt[C[A], A] =
    new TraversableOnceExt[C[A], A](coll, identity)

  implicit def commomExtendStringTraversable(string: String): TraversableOnceExt[String, Char] =
    new TraversableOnceExt[String, Char](string, implicitly)

  implicit def commomExtendArrayTraversable[A](array: Array[A]): TraversableOnceExt[Array[A], A] =
    new TraversableOnceExt[Array[A], A](array, implicitly)

  val splitString = "@@##@@"
  val dbMap = Map("accounting" -> "acct", "policy" -> "plcy", "contact" -> "cntct", "claims" -> "clms")

  val schema = StructType(
    List(
      StructField("ingestiondt", StringType, true),
      StructField("batch", StringType, true),
      StructField("TableName", StringType, true),
      StructField("SourceCount", LongType, true),
      StructField("LoadStartTime", StringType, true),
      StructField("LoadEndTime", StringType, true),
      StructField("LoadStatus", StringType, true),
      StructField("Exception", StringType, true),
      StructField("BatchWindowStart", StringType, true),
      StructField("BatchWindowEnd", StringType, true),
      StructField("QueryString", StringType, true),
      StructField("LoadType", StringType, true),
      StructField("BatchStartTime", StringType, true),
      StructField("TargetCount", LongType, true),
      StructField("HardDeleteBatch", StringType, true)
    )
  )

  val batchStatsSchema = StructType(
    List(
      StructField("processname", StringType, true),
      StructField("ingestiondt", StringType, true),
      StructField("batch", StringType, true),
      StructField("BatchStartTime", StringType, true),
      StructField("BatchWindowStart", StringType, true),
      StructField("BatchWindowEnd", StringType, true),
      StructField("HardDeleteBatch", StringType, true)
      /*,
      StructField("LoadType", StringType, true),
      StructField("RestartabilityInd", StringType, true),
      StructField("RestartabilityLevel", StringType, true)
      */
    )
  )

  def iterAction[A](iterator: Iterator[A]): List[A] = {
    def consolidate(iterator: Iterator[A], list: List[A]): List[A] = {
      iterator.hasNext match {
        case true => consolidate(iterator, List(iterator.next) ::: list)
        case false => list
      }
    }
    consolidate(iterator, List.empty)
  }
  val propertyMap: Map[String, String] = sqlQueryParserFromCSV
                      .getPropertyFile("diProperties.properties")
                      .map(props => (props.propertyName, props.PropValue)).toMap

  val targetDB: String = propertyMap.getOrElse("spark.DataIngestion.targetDB", "")
  val targetSecuredDB: String = propertyMap.getOrElse("spark.DataIngestion.targetSecuredDB", "")
  val oldConsolidateTypeList: String = targetDB  + "_typelist_consolidated"
  val newConsolidateTypeList: String = oldConsolidateTypeList + "_new"
  val auditPath: String = propertyMap.getOrElse("spark.DataIngestion.auditPath", "")
  val auditDB: String = propertyMap.getOrElse("spark.DataIngestion.auditDB", "")
  val hrmnzds3Path: String = propertyMap.getOrElse("spark.DataIngestion.hrmnzds3Path", "")
  val harmonizeds3SecurePath: String = propertyMap.getOrElse("spark.DataIngestion.hrmnzds3SecurePath", "")


  val processName: String = propertyMap.getOrElse("spark.DataIngestion.processName", "")
  val lookUpFile: String = propertyMap.getOrElse("spark.DataIngestion.lookUpFile", "")
  val tableFile: String = propertyMap.getOrElse("spark.DataIngestion.tableFile", "")
  val srcMissingDataTables: String = propertyMap.getOrElse("spark.DataIngestion.srcMissingDataTables", "")
  val loadType: String = propertyMap.getOrElse("spark.DataIngestion.loadType", "")
  val batchParallelism: String = propertyMap.getOrElse("spark.DataIngestion.batchParallelism", "")
  val batchEndCutOff: Array[String] = propertyMap.getOrElse("spark.DataIngestion.batchEndCutOff", "").split("-")
  val batchEndCutOffHour: Int = batchEndCutOff(0).toInt
  val batchEndCutOffMinute: Int = batchEndCutOff(1).toInt
  val metaInfoForLookupFile: MetaInfo = new MetaInfo(lookUpFile,tableFile)
  val metaInfoForTableFile: MetaInfo = new MetaInfo(lookUpFile,tableFile)
  val refTableListFromLookUp: Set[String] = metaInfoForLookupFile.getTableList
  val refTableListFromTableSpec: Iterator[String] = metaInfoForTableFile.getTypeTables
  val mainTableListFromTableSpec: List[String] = metaInfoForTableFile.getTableSpec.map(_._1)
  val refTableList: List[String] = (refTableListFromLookUp ++ refTableListFromTableSpec).toList.distinct
  val ref_col_list = metaInfoForLookupFile.getRefColList
  val refTableListStr: String = refTableList.mkString(splitString)
  val piiFile: String = propertyMap.getOrElse("spark.DataIngestion.piiFile", "")
  val piiListInfo: Map[String, List[(String, String)]] = new MetaInfo(piiFile,tableFile).getPiiInfoList
  val lookUpListInfo: Map[String, List[(String, String, String)]] = metaInfoForLookupFile.getLookUpData
  val timeZone: DateTimeZone = DateTimeZone.forID("America/New_York")
  def now = DateTime.now(timeZone)

  val datePartition: String = now.toString("YYYY-MM-dd")
  //val batch_start_time = now.toString("YYYY-MM-DD HH:mm:ss.sss")
  val restartabilityInd: String = propertyMap.getOrElse("spark.DataIngestion.restartabilityInd", "")
  val restartabilityLevel: String = propertyMap.getOrElse("spark.DataIngestion.restartabilityLevel", "").toLowerCase

  val stagePartitionBy: String = propertyMap.getOrElse("spark.DataIngestion.stagePartitionBy", "")
  val stageAuditPath: String = propertyMap.getOrElse("spark.DataIngestion.auditPath", "")
  val fromEmail: String = propertyMap.getOrElse("spark.DataIngestion.fromEmail", "")
  val toEmail: String = propertyMap.getOrElse("spark.DataIngestion.toEmail", "")
  val restartTableIdentifier: String = propertyMap.getOrElse("spark.DataIngestion.restartTableIdentifier", "failed")
  val stageAuditDB: String = propertyMap.getOrElse("spark.DataIngestion.auditDB", "")
  val stageAuditHiveTable: String = stageAuditDB + ".stage_audit"

  val partitionOverwriteMode: String = propertyMap.getOrElse("spark.DataIngestion.partitionOverwriteMode", "static")

  val piiListFromLookup: List[(String, String)] = piiListInfo.toList.flatMap(pii =>
    lookUpListInfo.get(pii._1) match {
      case Some(lkp) => lkp.map(lkpValues => (lkpValues._1, lkpValues._3))
      case None => None
    })

  val piiList: List[(String, String)] = (piiListFromLookup ++ piiListInfo.values.flatten.toList)
  val piiListMultiMap = piiList.toMultiMap
  //Holder.log.info("PiiList.info: " + piiList.toList)

  val tableInfos: Seq[(String, String, String, String)] = metaInfoForLookupFile.getLookUpInfo
  val tableGroup: Map[String, List[String]] = tableInfos.map(info =>
    (info._1.toString, info._2.toString + splitString + info._4.toString)).toList.toMultiMap

  val tableSpec: List[(String,String)] = metaInfoForTableFile.getTableSpec

  val tableIngestionList: List[String] = tableSpec.map(_._1)
  val deleteTableList: List[String] = tableSpec.map(x => (x._1,x._2.split(splitString,-1)(1))).filter(_._2 == "id").map(_._1)
  val tableSpecMap: Map[String, String] = tableSpec.toMap
  val tableSpecMapTrimmed: Map[String, String] = tableSpecMap.map{case (k,v) => k.split("\\.")(2).toLowerCase -> v}
  val refTableListfromTableSpec: String = metaInfoForTableFile.getTypeTables.mkString(splitString)

  val cdcQueryMap: Map[String, String] = new CdcQueryBuilder(propertyMap, tableSpec).draftCdcQuery()

  val considerBatchWindowInd: String = propertyMap.getOrElse("spark.DataIngestion.considerBatchWindow", "")

  val sourceDB: String = propertyMap.getOrElse("spark.DataIngestion.sourceDB", "")
  val connectDBInd: String = propertyMap.getOrElse("spark.DataIngestion.connectDBInd", "N")
  val sourceDBFormatted: String = if(propertyMap.getOrElse("spark.DataIngestion.sourceDB", "").contains("[") || propertyMap.getOrElse("spark.DataIngestion.sourceDB", "").contains("-"))
    propertyMap.getOrElse("spark.DataIngestion.sourceDB", "").replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_")
  else
    propertyMap.getOrElse("spark.DataIngestion.sourceDB", "")
  val repl_table: String = propertyMap.getOrElse("spark.DataIngestion.repl_table", "")

  val jdbcSqlConnStr: String = JdbcConnectionUtility.constructJDBCConnStr(propertyMap)
  val driver: String = JdbcConnectionUtility.getJDBCDriverName(propertyMap)

  val timeLagInMins: Int = propertyMap.getOrElse("spark.DataIngestion.timeLagInMins", 0).asInstanceOf[String].toInt

  val hardDeleteBatch: String = propertyMap.getOrElse("spark.DataIngestion.hardDeleteBatch", "N")

  val incrementalpartition: String = propertyMap.getOrElse("spark.DataIngestion.incrementalPartition", "1")

  val schemaCheck: Boolean = if(propertyMap.getOrElse("spark.DataIngestion.schemaCheck", "false") == "true") true else false
  val broadcastTimeout = propertyMap.getOrElse("spark.sql.broadcastTimeout", "30000000")
  val autoBroadcastJoinThreshold = propertyMap.getOrElse("spark.sql.autoBroadcastJoinThreshold", "-1")
  val validationBeforeHDRequired = propertyMap.getOrElse("spark.ingestion.validationBeforeHD", "true")
 /*
 * EDIN-362: Create empty hive table in harmonized layer for TL. Passing this switch variable "emptyTableLoadRequired" from property file.
 *By default this value is true for this variable.
 */
  val emptyTableLoadRequired = propertyMap.getOrElse("spark.ingestion.emptytableLoadFlag", "true")
  //EDIN-406
  val claimcenterDatabaseName = propertyMap.getOrElse("spark.ingestion.claimcenter.database.name", "[ClaimCenterL3-1ENT]")

  //Stage Load Parameters
  val stgLoadBatch: Boolean = propertyMap.getOrElse("spark.ingestion.stageLoadBatch", "false") == "true"
  val stgFilename: String = propertyMap.getOrElse("spark.ingestion.stageTableList","")
  val stgTableList: List[String] = stgLoadBatch match {
    case true => Source.fromFile(stgFilename).getLines.toList.map(_.toLowerCase)
    case _ => List.empty[String]
  }
  val s3SyncEnabled: Boolean = propertyMap.getOrElse("spark.ingestion.s3SyncEnabled","false") == "true"
  val loadOnlyTLBatch: Boolean = propertyMap.getOrElse("spark.ingestion.loadOnlyTLBatch","false") == "true"
  val loadFromStage: Boolean = propertyMap.getOrElse("spark.ingestion.loadFromStage","false") == "true"
  val extractProcessName: String = propertyMap.getOrElse("spark.ingestion.extractProcessName", "")
  val restoreFromBatch: String = propertyMap.getOrElse("spark.ingestion.restoreFromBatch", "")
  val stageTablePrefix: String = propertyMap.getOrElse("spark.DataIngestion.stageTablePrefix","")

  val hiveDB: String = stgLoadBatch match {
    case true =>  propertyMap.getOrElse ("spark.DataIngestion.targetStageDB", "")
    case false => propertyMap.getOrElse ("spark.DataIngestion.targetDB", "")
  }
  val hiveSecuredDB: String = stgLoadBatch match {
    case true => propertyMap.getOrElse("spark.DataIngestion.targetSecuredStageDB", "")
    case false => propertyMap.getOrElse("spark.DataIngestion.targetSecuredDB", "")
  }
  val s3Location: String = stgLoadBatch match {
    case true => propertyMap.getOrElse("spark.DataIngestion.stageS3Path", "") + "/temp/"
    case false => propertyMap.getOrElse("spark.DataIngestion.hrmnzds3Path", "") + "/"
  }
  val s3SecuredLocation: String = stgLoadBatch match {
    case true => propertyMap.getOrElse("spark.DataIngestion.stageS3SecurePath", "") + "/temp/"
    case false => propertyMap.getOrElse("spark.DataIngestion.hrmnzds3SecurePath", "")  + "/"
  }

  def formatDBName(originalDBName: String) : String = {
    if(originalDBName.contains("-"))
      originalDBName.replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_")
    else
      originalDBName
  }

  def getEnvironment(auditDB : String) : String = {
    if (auditDB.toLowerCase.trim.contains("dev")) "Development"
    else if (auditDB.toLowerCase.trim.contains("uat")) "UAT"
    else "Production"
  }

  val isConnectDatabase: Boolean =  propertyMap.getOrElse("spark.ingestion.isconnectdatabase", "false") == "true"
  val isTypeListToBeRefreshed: Boolean =  propertyMap.getOrElse("spark.ingestion.isTypeListToBeRefreshed", "false") == "true"

  val reconResultDB: String =  propertyMap.getOrElse("spark.ingestion.reconResultDB", "edf_recon")
  val transformDB: String =  propertyMap.getOrElse("spark.ingestion.transformDB", "edf_transformed")
  val reconResultPath: String =  propertyMap.getOrElse("spark.ingestion.reconResultPath", "")



  /*
  *  def deleteStagePartition: Unit = {
      val stageAuditData = spark.sql(s"select * from $stageAuditHiveTable where where processName = '$processName' " +
        s"and  stagingStatus = 'InProgress' and stagingDate in (" +
        s"select max(stagingDate) from $stageAuditHiveTable where processName = '$processName'")
      val filePattern = raw"s3://([^/]+)/(.*)".r
      val fileListStr = stageAuditData.
        select(col("fileName")).rdd.collect
        .map(file => {
          val fileStr = file.getAs[String](0)
          fileStr match {
            case filePattern(bucket, file) => Some(bucket, file)
            case _ => None
          }
        }).toList
      val bucketName = fileListStr.head.get._1
      val fileList = fileListStr.map(_.get._2).mkString("\n")

      val awsDeleteCmd = scala.sys.process.Process(s"""echo $fileList | xargs -P8 -n1000 bash -c " +
        s"'aws s3api delete-objects --bucket $bucketName --delete
        "Objects=[$$(printf "{Key=%s}," "$$@")],Quiet=false"' _""")
      val filesDeleted = awsDeleteCmd.!!
      Holder.log.info("Stage File Deleted: " + filesDeleted)
      stageAuditData
        .withColumn("stagingStatus", lit("Complete"))
        .write.format("parquet").mode(SaveMode.Append)
        .partitionBy("processName","stagingDate")
        .bucketBy(50, "tableName")
        .options(Map("path"-> (stageAuditPath + "/stageAudit")))
        .saveAsTable(stageAuditHiveTable)
    }*/
}
