package edf.missingdataload

import edf.dataingestion.SparkJob
import edf.dataingestion._
import edf.utilities.{Holder, TraversableOnceExt}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, max}

import scala.language.higherKinds
import org.joda.time.format.DateTimeFormat

import scala.collection.immutable.Map
import scala.io.Source
import scala.util.{Failure, Success, Try}

object FindAndLoadMissingRecords extends SparkJob {

  implicit lazy val implicitConversions = scala.language.implicitConversions
  implicit def commomExtendTraversable[A, C[A] <: TraversableOnce[A]](coll: C[A]): TraversableOnceExt[C[A], A] =
    new TraversableOnceExt[C[A], A](coll, identity)
  implicit def commomExtendStringTraversable(string: String): TraversableOnceExt[String, Char] =
    new TraversableOnceExt[String, Char](string, implicitly)
  implicit def commomExtendArrayTraversable[A](array: Array[A]): TraversableOnceExt[Array[A], A] =
    new TraversableOnceExt[Array[A], A](array, implicitly)

  val tableGroup: Map[String, List[String]] = tableInfos.map(info =>
    (info._1.split("\\.")(2), info._2.toString + splitString + info._4.toString)).toList.toMultiMap
  val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.sss")

  override def run(sprkSession: SparkSession, propertyConfigs: Map[String, String]): Unit = {
    implicit val spark: SparkSession = sprkSession
    //spark.conf.set("spark.scheduler.mode", "FAIR")
    spark.conf.set("spark.sql.thriftserver.scheduler.pool", "accounting")
    spark.conf.set("spark.scheduler.pool","production")

    val s3AuditPath: String = auditPath.replace("(?i)dataingestion", "audit_missing_data")
    val auditTable = s"$auditDB.audit_missing_data"

    val current_time = datetime_format.
      parseDateTime(now.toString("YYYY-MM-dd HH:mm:ss.sss")).
      withZone(timeZone)
    implicit val missingDataBatch: String = current_time.getMillis.toString

    /**********************************
    possible values are
    identify - only identify missing data,
    load - load the missing data from the previous run,
    both - identify and load the missing data
    *************************************/
    val missingDataAction = propertyMap.getOrElse("spark.DataIngestion.missingDataAction", "both")

    // TODO If the missing file is not provided, read the table list from spec file.
    val missingRecordTableList = Source.fromFile(srcMissingDataTables).getLines().toList

    Try {spark.sql(s"select * from $auditDB.${oldConsolidateTypeList.replace("_data_processed", "")}")
      .coalesce(1)
      .cache()
      .createOrReplaceTempView(oldConsolidateTypeList)
    } match {
      case Success(x) => Holder.log.info("Consoldated Type List Loaded")
      case Failure(exception) => Holder.log.info("Consolidated Type List failed:\n "+ exception.getMessage)
    }
    import org.apache.spark.sql.functions.lit
    val maxData = spark.sql(s"select tableName, batchwindowend, harddeletebatch from edf_dataingestion.audit " +
      s"where processName='gwplDataExtract' ").
      where(col("harddeletebatch")===lit('N')).
      groupBy(col("tableName")).agg(max(col("batchwindowend"))).cache

    /*************************************************************************************************
    1. Draft the source query to read entire dataset containing
          partitionByCol & cdcCol from sqlServer.
    2. Using the query and partition options from step 1 and draft a source spark dataFrame reader.
    3. Execute the query against the respective database for each of the table.
       Compare the source and target dataset and get the missing records.
     4. Source the missing records from target and insert into datalake.
    **************************************************************************************************/

    missingDataAction match {
      case "both" =>
        val auditRowSeq = missingRecordTableList.par.
          map(DraftQuery.draftSrcQueryWithPartitions(_)).par.
          map(CreateDataFrameReader.createDFReader(_)).toMap.par.
          map(MissingRecords.identifyMissingRecords(_, maxData)).par. //Compare source with Target and identify missing ids.
          filter(!_._2._1.isEmpty).par.                      //Filter the tables that doesn't have any missing ids.
          map(BuildDataFrame.buildMissingDataSet(_)).par.    //For the list of missing IDS, get the records from harmonized
          map(IngestData.ingestMissingRecords).              //ingest the records to the respective table in harmonized.
          foldLeft(Seq[Row]())((acc, row) => acc :+ row)

        AuditData.buildAuditData(auditRowSeq, s3AuditPath, auditTable)
      case "identify" =>
        val missingRecords = missingRecordTableList.par.
          map(DraftQuery.draftSrcQueryWithPartitions(_)).par.
          map(CreateDataFrameReader.createDFReader(_)).toMap.par.
          map(MissingRecords.identifyMissingRecords(_,maxData)).par
        val auditRowSeq = missingRecords.foldLeft(Seq[Row]())((acc, data) => {
          val missingRecordsDetails = data._2
          val row = Row(processName, data._1, missingRecordsDetails._1, missingRecordsDetails._2.toString, null, null,
            current_time.toString, "N", missingDataBatch)
          acc :+ row
        })
        AuditData.buildAuditData(auditRowSeq, s3AuditPath, auditTable)
      case "load" =>
        //TODO Handle scenario to do a load after identify batch..<both vs load>
        val loadMissingDataBatchArr: Array[String] =
          propertyMap.getOrElse("spark.DataIngestion.missingDataBatchToLoad", "") match {
            case "" => spark.sql(s"select tablename, missingdatabatch from $auditTable where processname = '$processName' " +
              s" and insertind == 'N'  MINUS select tablename, missingdatabatch from $auditTable where processname = '$processName' " +
              s" and insertind == 'Y'").rdd.map(_.getString(1)).collect
            case batch: String => Array(batch)
          }
        val loadMissingDataBatch =
          if(loadMissingDataBatchArr.isEmpty) "" else
                            loadMissingDataBatchArr.mkString(",")
        val missingData = spark.sql(s"select tablename, missingids, cutoffvalue, missingDataBatch from $auditTable " +
          s"where  processname = '$processName' and missingDataBatch in ($loadMissingDataBatch) ")
        val auditRowSeq = missingData.filter(col("missingids") =!= "").rdd.collect.
          map(row => (row.getString(0), (row.getString(1), row.getString(2), row.getString(3)))).toList.par
          .map(BuildDataFrame.buildMissingDataSet(_)).par. //For the list of missing IDS, get the records from harmonized
          map(IngestData.ingestMissingRecords).
          foldLeft(Seq[Row]())((acc, row) => acc :+ row)
        AuditData.buildAuditData(auditRowSeq, s3AuditPath, auditTable)
    }
  }
}
