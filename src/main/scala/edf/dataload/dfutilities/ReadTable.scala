package edf.dataload.dfutilities

import edf.dataload.{cdcQueryMap, claimcenterDatabaseName,
  hardDeleteBatch, loadType, mainTableListFromTableSpec,
  now, propertyMap, refTableListfromTableSpec,
  splitString, tableSpecMap, timeZone, dbType}
import edf.dataload.helperutilities.PartitionBounds
import edf.utilities.{Holder, JdbcConnectionUtility}
import org.apache.spark.sql.SparkSession
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

object ReadTable {
  def readTable( tableName: String, cacheInd: Char, batch_start_time: String,
                 PreviousBatchWindowStart: String, PreviousBatchWindowEnd: String,
                 failedPartition: String)(
               implicit spark: SparkSession) = {

    //val dateInstance = Calendar.getInstance(TimeZone.getTimeZone("EST"))
    val load_start_time = now.toString("YYYY-MM-dd HH:mm:ss.sss")
    val datePartition = now.toString("YYYY-MM-dd")
    val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.sss")
    val nowMillis = datetime_format.parseDateTime(batch_start_time).withZone(timeZone).getMillis
    val restartInd = propertyMap.getOrElse("spark.DataIngestion.restartabilityInd", "")
    val considerBatchWindow = if (hardDeleteBatch == "Y")
      'Y' else
      propertyMap.getOrElse("spark.DataIngestion.considerBatchWindow", "")
    val batchPartition = restartInd match {
      case "Y" => failedPartition
      case _ => nowMillis.toString
    }

    val (jdbcSqlConnStr, driver) = if (cacheInd == 'Y') {
      val sqlConnStr = JdbcConnectionUtility.constructTypeListJDBCConnStr(propertyMap)
      val jdbcDriver = JdbcConnectionUtility.getTLJDBCDriverName(propertyMap)
      (sqlConnStr, jdbcDriver)
    }
    else {
      val sqlConnStr = JdbcConnectionUtility.constructJDBCConnStr(propertyMap)
      val jdbcDriver = JdbcConnectionUtility.getJDBCDriverName(propertyMap)
      (sqlConnStr, jdbcDriver)
    }
    val df_name_arr = tableName.split("\\.")
    val formattedDBName =if(df_name_arr(0).contains("-"))
      df_name_arr(0).replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_")
    else
      df_name_arr(0)

    val df_name = restartInd match {
      case "Y" if cacheInd != 'Y' =>  formattedDBName + "_" + df_name_arr(2) + "_" + batchPartition
      case _ => formattedDBName + "_" + df_name_arr(2)

    }
    //val lookUpFile = propertyMap.getOrElse("spark.dataingestion.lookUpFile", "")
    //val tableFile: String = propertyMap.getOrElse("spark.dataingestion.tableFile", "")

    val cdcColFromTableSpecStr = tableSpecMap.getOrElse(tableName, "").split(splitString, -1)
    val (cdcColFromTableSpec, hardDeleteFlag) = if (cacheInd != 'Y') {
      cdcColFromTableSpecStr match {
        case Array(x,y,_,_,_) => (x, y )
        case _ => throw new Exception(s"parse Exception for table $tableName" +
          s" using ${cdcColFromTableSpecStr.mkString("-")}")
      }
    } else ("", "")

    def parseQueryAndOptions =
      if (cacheInd == 'Y') {
        Success((s"(select * from $tableName ) $df_name", Map("" -> "")))
      } else {
        val (cdcQuery: String, cdcCols: String) = cdcQueryMap.getOrElse(df_name_arr(2), "").split("\\:") match {
          case Array(query, cdcField) => {
          //  Holder.log.info("####Capturing cdcQueryMapInfo inside read Table: " + query.toString + " : " + cdcField.toString)
            (query.toString, cdcField.toString)
          }
          case _ => ("", "")
        }

        val deleteRecordFilter = "__$operation = 1"
        val decryptColList = cdcColFromTableSpecStr(4).split(",",-1)
        //EDIN-406
        def decryptFunction(colName: String) = s"CASE WHEN $colName > '' THEN ${claimcenterDatabaseName}.dbo.AESDecrypt($colName,${claimcenterDatabaseName}.dbo.fnGetKeyAESKey()) ELSE NULL END AS ${colName}_decrypted, "
        val decryptCol = decryptColList match {
          case Array("") => ""
          case other => other.map(decryptFunction(_)).reduce(_+_)
        }

        /* loadType defines between truncate and load or incremental load */
        if (loadType == "TL") {
          val boundQuery = if(dbType == "mysql")
            s"(${cdcQuery.replaceAll(".dummy.",".")}) bounds"
          else s"(${cdcQuery}) bounds"
           Holder.log.info("####Capturing queryInfo inside read Table-Truncate: " + boundQuery)
          val conditionForQuery = considerBatchWindow match {
            case "Y" => if (hardDeleteFlag == "cdc" && tableName.contains(".cdc."))
              s" where ${cdcColFromTableSpec} <= '${PreviousBatchWindowEnd}' and $deleteRecordFilter "
            else
              s" where ${cdcColFromTableSpec} <= '${PreviousBatchWindowEnd}' "
            case "N" => restartInd match {
              case "Y" => if (hardDeleteFlag == "cdc" && tableName.contains(".cdc."))
                s" where $deleteRecordFilter " else ""
              case "N" => if (hardDeleteFlag == "cdc" && tableName.contains(".cdc."))
                s" where $deleteRecordFilter " else ""
            }
          }
              val partitionMap: Map[String, String] = PartitionBounds(tableName, boundQuery, cdcColFromTableSpecStr)
              if(dbType == "mysql")
              Success(s"(select  id, `datetime`, `msec`, `sessionID`, `user_token`, customerid, portalid, quoteid, `page`, fld_label, fld_value, origin, referer, clickdata " +
                s"from `ClickstreamL4`.`ingest` where portalid = 'SPA-Personal') ${df_name}", partitionMap)
              else
              if (cdcColFromTableSpec.trim == "" || cdcColFromTableSpec.contains("truncate") || cdcColFromTableSpec.endsWith("id")) {
                Success(s"(select cast(null as datetime) as deleteTime, $decryptCol * from ${tableName} ) ${df_name}", partitionMap)
              } else if (hardDeleteFlag == "cdc" && tableName.contains(".cdc.")) {
                Success(s"(select  ${cdcColFromTableSpec} deleteTime, $decryptCol * from ${tableName}  $conditionForQuery) ${df_name}", partitionMap)
              } else
                Success(s"(select  cast(null as datetime) as deleteTime, $decryptCol * from ${tableName} $conditionForQuery ) ${df_name}", partitionMap)
        } else {
          val partitionMap: Map[String, String] = Map("" -> "")
          val conditionForQuery = considerBatchWindow match {
            case "Y" if hardDeleteFlag == "cdc" && tableName.contains(".cdc.") =>
              s" where $cdcColFromTableSpec > '$PreviousBatchWindowStart' and " +
                s"$cdcColFromTableSpec <= '$PreviousBatchWindowEnd' and $deleteRecordFilter "
            case "Y" => s" where $cdcColFromTableSpec > '$PreviousBatchWindowStart' and " +
              s" $cdcColFromTableSpec <= '$PreviousBatchWindowEnd' "
            case "N" => restartInd match {
              case "Y" if hardDeleteFlag == "cdc" && tableName.contains(".cdc.") =>
                s" where $cdcColFromTableSpec > '$PreviousBatchWindowStart' and " +
                  s"$cdcColFromTableSpec <= '$PreviousBatchWindowEnd' and $deleteRecordFilter "
              case "Y" => s" where $cdcColFromTableSpec > '$PreviousBatchWindowStart' and " +
                s"$cdcColFromTableSpec <= '$PreviousBatchWindowEnd'"
              case "N" if hardDeleteFlag == "cdc" && tableName.contains(".cdc.") =>
                s" where $cdcColFromTableSpec > '$PreviousBatchWindowEnd' and " +
                  s"$deleteRecordFilter "
              case "N" => s" where $cdcColFromTableSpec > '$PreviousBatchWindowEnd'"
            }
          }
          //Holder.log.info("refTableList: " + refTableList)
          //Holder.log.info("mainTableList: " + mainTableListFromTableSpec.mkString(","))
          if (refTableListfromTableSpec.contains(tableName)) {
            val querytoExecute = s"(select cast(null as datetime) as deleteTime, $decryptCol * from ${tableName}) ${df_name}"
            Success(querytoExecute, partitionMap)
          }
          else if (cdcCols.endsWith("id") || cdcCols.endsWith("yyyymm") || cdcCols.startsWith("loadtime")) {
            /*val maxCdcDF: Try[org.apache.spark.sql.Row] = Try {
              spark.sqlContext.sql(cdcQuery).first
            }*/
            val maxCDF = if (PreviousBatchWindowEnd.trim.isEmpty || PreviousBatchWindowEnd == "1900-01-01" ||
              (refTableListfromTableSpec.contains(tableName) && mainTableListFromTableSpec.contains(tableName))) "0" else PreviousBatchWindowEnd
            //Holder.log.info("####Capturing queryInfo inside read Table - Incremental: " + s"(select * from ${tableName} where ${cdcCols} > ${Cdc} ) ${df_name}")
            val partitionMap: Map[String, String] = Map("" -> "")
            Success(s"(select cast(null as datetime) as deleteTime, $decryptCol * from ${tableName} where ${cdcCols} > '${maxCDF}' ) ${df_name}", partitionMap)
          }
          else if (hardDeleteFlag == "cdc" && tableName.contains(".cdc.")) {
            val queryToExecute = s"(select  ${cdcCols} deleteTime, $decryptCol * from ${tableName} $conditionForQuery) ${df_name}"
            Success(queryToExecute, partitionMap)
          }
          else {
            val queryToExecute = s"(select cast(null as datetime) as deleteTime, $decryptCol * from ${tableName} $conditionForQuery ) ${df_name}"
            Success(queryToExecute, partitionMap)
          }
        }
      }

    def dfReaderTry = parseQueryAndOptions match {
      case Success((query, optns: Map[String, String])) => {
        val numPartitions = optns.getOrElse("numPartitions", "1")
        val queryToExecute = if(dbType=="mysql")
          query.
            replaceAll(".dummy.",".")
        else query
        Holder.log.info("The query to be executed : " + queryToExecute)
        Try(spark.sqlContext.read.format("jdbc")
          .options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> queryToExecute) ++ optns), query, numPartitions
        )
      }
    }
    def stats = dfReaderTry match {
      case Success(dfReaderAndQuery) => {
        val failedQuery = dfReaderAndQuery._2
        val df = Try {
          dfReaderAndQuery._1.load()
        }


        val df_statistics = df match {
          case Success(dfData) => {
            if (cacheInd == 'Y') {
              val dfFields = dfData.schema.fieldNames.sorted
              dfData.select(dfFields.head, dfFields.tail: _*).createOrReplaceTempView(df_name)
              //Holder.log.info(df_name + ": " + dfData.count())
              val load_end_time = now.toString("YYYY-MM-dd HH:mm:ss.sss")
              val dfCount = 0L
              Success(datePartition + splitString + batchPartition + splitString +
                tableName + splitString + dfCount + splitString + load_start_time +
                splitString + load_end_time + splitString + "success" + splitString + "-"
                + splitString + PreviousBatchWindowStart + splitString + PreviousBatchWindowEnd + splitString + dfReaderAndQuery._2 +
                splitString + loadType + splitString + batch_start_time)
            }
            else {
              //val partColsStr = tableSpecMap.getOrElse(tableName, "")
              val df = if (refTableListfromTableSpec.contains(tableName) && !mainTableListFromTableSpec.contains(tableName)) {
                //Holder.log.info("I am here loding df: ")
                dfReaderAndQuery._1.load()
              } else  {
                import org.apache.spark.sql.functions.lit
                dfReaderAndQuery._1.load()
                  .withColumn("ingestiondt", lit(datePartition))
                  .withColumn("batch", lit(batchPartition.toString))
              }

              if (refTableListfromTableSpec.contains(tableName) && mainTableListFromTableSpec.contains(tableName)) {
                //Holder.log.info("I am here loding df: ")
                df.cache().createOrReplaceTempView(df_name)
              }

              val (min_window, max_window) = if (considerBatchWindow == "Y")
                (PreviousBatchWindowStart, PreviousBatchWindowEnd)
              else
                (null, null)

              val operationalFields = df.schema.fieldNames.filter(_.contains("$"))
              //Holder.log.info(df_name + ": " + df.count())
              if(loadType == "TL")
                if (refTableListfromTableSpec.contains(tableName) && mainTableListFromTableSpec.contains(tableName))
                  df.drop(operationalFields: _*).cache.createOrReplaceTempView(df_name)
                else
                  df.drop(operationalFields: _*).createOrReplaceTempView(df_name)
              else {
                df.drop(operationalFields: _*).cache.createOrReplaceTempView(df_name)
                spark.table(df_name).count
              }

              val srcCount = 0L //spark.sql(s"select count(*) from $df_name").first().getAs[Long](0)
              //Holder.log.info("df_name view Created")
              val load_end_time = now.toString("YYYY-MM-dd HH:mm:ss.sss")
              Success(datePartition + splitString + batchPartition + splitString +
                tableName + splitString + srcCount + splitString +
                load_start_time + splitString + load_end_time + splitString + "success" + splitString + "-"
                + splitString + min_window + splitString + max_window + splitString + dfReaderAndQuery._2 +
                splitString + loadType + splitString + batch_start_time)
            }
          }
          case Failure(msg) =>
            Failure(new Throwable(datePartition + splitString + batchPartition + splitString +
              tableName + splitString + 0 + splitString + load_start_time + splitString +
              now.toString("YYYY-MM-dd HH:mm:ss.sss") + splitString + "failed" + splitString + msg.getMessage
              + splitString + PreviousBatchWindowStart + splitString + PreviousBatchWindowEnd +
              splitString + failedQuery + splitString + loadType + splitString + batch_start_time))
        }
        df_statistics
      }
      case Failure(msg) =>
        val msgStr = msg.getMessage.split(splitString, -1)
        val (errorMsg, query) = (msgStr(0), msgStr(1))
        Failure(new Throwable(datePartition + splitString + batchPartition + splitString +
          tableName + splitString + 0 + splitString + load_start_time + splitString +
          now.toString("YYYY-MM-dd HH:mm:ss.sss") + splitString + "failed" + splitString + errorMsg
          + splitString + PreviousBatchWindowStart + splitString + PreviousBatchWindowEnd + splitString + query
          + splitString + loadType + splitString + batch_start_time))
    }
    if (hardDeleteBatch == "Y" && hardDeleteFlag == "id") {
      Try {
        BuildDeleteRecords(tableName, cdcColFromTableSpecStr, datePartition, batchPartition.toString)
      } match {
        case Success(_) =>
          Success(datePartition + splitString + batchPartition + splitString +
            tableName + splitString + 0L + splitString +
            load_start_time + splitString + now.toString("YYYY-MM-dd HH:mm:ss.sss") + splitString + "success" + splitString + "-"
            + splitString + PreviousBatchWindowStart + splitString + PreviousBatchWindowEnd + splitString + null +
            splitString + loadType + splitString + batch_start_time)
        case Failure(exception) =>
          Failure(new Throwable(datePartition + splitString + batchPartition + splitString +
            tableName + splitString + 0 + splitString + load_start_time + splitString +
            now.toString("YYYY-MM-dd HH:mm:ss.sss") + splitString + "failed" + splitString + exception.getMessage +
            "\n" + exception.getStackTrace + splitString + PreviousBatchWindowStart + splitString + PreviousBatchWindowEnd +
            splitString + null + splitString + loadType + splitString + batch_start_time))
      }
    }
    else stats
  }

  def apply( tableName: String, cacheInd: Char, batch_start_time: String,
             previousBatchWindowStart: String, previousBatchWindowEnd: String,
             failedPartition: String)(
             implicit spark: SparkSession) = {
    readTable(tableName, cacheInd, batch_start_time,previousBatchWindowStart, previousBatchWindowEnd, failedPartition)
  }
}
