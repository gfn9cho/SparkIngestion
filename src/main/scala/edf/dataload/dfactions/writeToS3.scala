package edf.dataload.dfactions

import edf.dataload._
import edf.utilities.Holder
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.functions.{col, input_file_name, lit, pmod}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.immutable.Map
import scala.sys.process._

object writeToS3 {
  def insertInToS3(df: DataFrame , hardDeleteDF: DataFrame , s3TempLocation: String, hiveTable: String,
                   saveMode: SaveMode, batchPartition: String, cdcColMax: String, tableLoadType: String)
                  (implicit  spark: SparkSession): (Long, Long) = {
    /*
      * EDIN-362: Create empty hive table in harmonized layer for TL
    */
    val isDeleteTable =  if (loadType != "RB")
      deleteTableList.map(_.split("\\.")(2)).
      contains(hiveTable.split("\\.")(1).replaceAll(stageTablePrefix,""))
    else false
    val loadTable: Boolean =
                    if(loadType == "TL" || loadType == "RB" || isDeleteTable) true
                    else !df.head(1).isEmpty
    def hudiOrSparkWrite(df: DataFrame): Unit = {
      stgLoadBatch match {
        case true if loadType == "TL" || tableLoadType == "TL" =>
          val partNum = df.select("ingestiondt").dropDuplicates().count
          val dfFiltered = if(isDeleteTable)
                            df.filter(col("deleted_flag")===0) else df
          dfFiltered
            .withColumn("batch", lit(batchPartition))
            .withColumn("bucket", pmod(col("id"), lit(10)))
            .repartition(col("ingestiondt"), col("bucket"), lit(partNum ))
            .write.format("parquet")
            .partitionBy("ingestiondt", "bucket")
            .options(Map("path" -> s3TempLocation))
            .mode(SaveMode.Overwrite)
            .saveAsTable(hiveTable)
          s3BackUp(true)
        case true if loadType == "RB" =>
          Holder.log.info(s"Restoring batch from $restoreFromBatch")
          s3RestoreFromBackUp(restoreFromBatch)
        case true =>
            s3BackUp(false)
            StageIncremental.identifyPartitions(df, hardDeleteDF, hiveTable,
              batchPartition, isDeleteTable, s3TempLocation)
            df
              .withColumn("bucket", pmod(col("id"), lit(10)))
              .write
              .partitionBy("ingestiondt", "bucket")
              .mode(saveMode)
              .parquet(s3TempLocation)
            StageIncremental.duplicatesCheck(hiveTable, s3TempLocation)
            s3BackUp(true)
        case false =>
          Holder.log.info("Executing spark load")
          df
            .write.format("parquet")
            .partitionBy("ingestiondt", "batch")
            .options(Map("path" -> s3TempLocation))
            .mode(saveMode)
            .saveAsTable(hiveTable)
      }
    }

    def s3BackUp(isLoadCompleteInd: Boolean) = {
      val s3LocPattern = "^(.*)(/[A-Za-z_]+)/?$".r
      val s3PrimLocation = s3TempLocation.replaceAll("/temp/","/")
      val backUpLocation = s3LocPattern.replaceAllIn(s3PrimLocation, s"$$1/${batchPartition}$$2")

      if(isLoadCompleteInd) {
        s"aws s3 sync $s3TempLocation $s3PrimLocation --delete".!!
      } else if (s3SyncEnabled) {
        if (restartabilityInd == "Y") {
          s"aws s3 sync $s3PrimLocation $s3TempLocation --delete".!!
          s"aws s3 sync $s3PrimLocation $backUpLocation --delete".!!
        } else
          s"aws s3 sync $s3TempLocation $backUpLocation".!!
      }

      if(loadType == "TL" || tableLoadType == "TL") {
        createHiveTable(s3PrimLocation)
        spark.sql(s"ALTER TABLE $hiveTable RECOVER PARTITIONS")
      } else {
        spark.sql(s"ALTER TABLE $hiveTable RECOVER PARTITIONS")
      }
      //s3Sync.!!
    }

    def createHiveTable(s3Path: String) = {
      val schemaCopy= spark.read.table(hiveTable).schema.fields.
        map(field => field.name + " " + field.dataType.typeName).mkString(",")
      spark.sql(s"drop table $hiveTable")
      Holder.log.info("create table: " + s"""CREATE TABLE $hiveTable($schemaCopy)
                                            |USING parquet
                                            |OPTIONS (path='$s3Path')
                                            |PARTITIONED BY (ingestiondt, bucket)
                                            |""".stripMargin)
      spark.sql(
        s"""CREATE TABLE $hiveTable($schemaCopy)
           |USING parquet
           |OPTIONS (path='$s3Path')
           |PARTITIONED BY (ingestiondt, bucket)
           |""".stripMargin)
    }

    def s3RestoreFromBackUp(restoreBatch: String) = {
      val s3PrimLocation = s3TempLocation.replaceAll("/temp/","/")
      val backUpLocation = s"$s3PrimLocation$restoreBatch"
      Holder.log.info("restore batch: " + s"aws s3 sync $backUpLocation $s3TempLocation")
      Holder.log.info("restore batch: " + s"aws s3 sync $backUpLocation $s3PrimLocation")
      s"aws s3 sync $backUpLocation $s3TempLocation --delete".!!
      s"aws s3 sync $backUpLocation $s3PrimLocation --delete".!!

      //TODO -- add backup of audit data
      spark.sql(s"REFRESH TABLE $auditDB.audit")
      val auditDF = spark.sql(s"select * from $auditDB.audit where " +
        s"processname='$processName' and batch > '$restoreFromBatch'")
        .withColumn("fileToDelete", input_file_name())
        .select(col("fileToDelete"))
        .dropDuplicates().collect().map(_.getString(0))
      Holder.log.info("restore audit"+ auditDF.mkString(","))
      if (!auditDF.isEmpty) StageIncremental.deleteStagePartition(auditDF.toList)
    }


    def getHudiOptions = {
      val Array(hiveDBName, hiveTableName) = hiveTable.split("\\.")
      Holder.log.info("cdcColMax: " + cdcColMax)
      Holder.log.info("hiveDBName: " + hiveDBName)
      Holder.log.info("hiveTableName: " + hiveTableName)

      val hudiOption =  Map[String, String](
        HoodieWriteConfig.TABLE_NAME -> hiveTableName,
        DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY -> "COPY_ON_WRITE",
        DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "ID",
        DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY ->"ingestiondt",
        DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "UpdateTime",
        DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
        DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> hiveDBName,
        DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> hiveTableName,
        DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> "ingestiondt"
      )
      loadType match {
        case "TL" => hudiOption +
          (DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        case _ if hardDeleteBatch == "Y" => hudiOption +
          (DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
            DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY -> "org.apache.hudi.EmptyHoodieRecordPayload")
        case _ => hudiOption +
          (DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      }
    }
    if(loadTable) {
      if (loadType == "TL") hudiOrSparkWrite(df) else hudiOrSparkWrite(df.cache)
       if(loadType == "RB")  (0L,0L) else {
         val count = Array("src", "tgt").par.map {
          case dest if dest == "src" => if(stgLoadBatch && isDeleteTable)
            df.filter(col("deleted_flag")===0).count else df.count
          case dest if dest == "tgt" => spark.sql(s"select count(*) as df_count from $hiveTable " +
            s"where batch = '$batchPartition'").first().getAs[Long]("df_count")
        }
        (count.head, count.last)
      }
    } else (0L,0L)
  }

  def apply(df: DataFrame, hardDeleteDF: DataFrame, s3TempLocation: String, hiveTable: String, saveMode: SaveMode,
            batchPartition: String, cdcColMax: String, tableLoadType: String)
           (implicit  spark: SparkSession): (Long, Long) = {
      insertInToS3(df, hardDeleteDF, s3TempLocation, hiveTable,
        saveMode, batchPartition, cdcColMax, tableLoadType)
  }
}



/**
Holder.log.info("Executing hudi load")
          val hudiOptions = getHudiOptions
          df.withColumn("ingestiondt",
            regexp_replace(col("ingestiondt"),lit("-"),lit("/")))
            .write.format("org.apache.hudi")
            .mode(saveMode)
            .options(hudiOptions)
            .save(s3Location)

  **/