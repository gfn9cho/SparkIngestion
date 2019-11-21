package edf.dataingestion

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.joda.time.format.DateTimeFormat
import edf.utilities.{Cipher, Holder, JdbcConnectionUtility, MetaInfo}
import org.apache.spark.sql.functions.{col, lit, broadcast}
import org.apache.spark.sql.functions.{col, max, min}
object DataFrameLoader extends Serializable {

  val dbMap = Map("accounting" -> "acct", "policy" -> "plcy", "contact" -> "cntct", "claims" -> "clms")


  def readTable(spark: SparkSession, tableName: String, propertyConfigs: Map[String, String],
                cacheInd: Char, batch_start_time: String, PreviousBatchWindowStart: String,
                PreviousBatchWindowEnd: String, failedPartition: String, hardDeleteBatch: String,
                tableSpecMap: Map[String,String], refTableList: String, cdcQueryMap: Map[String,String]) = {

    //val dateInstance = Calendar.getInstance(TimeZone.getTimeZone("EST"))
    val load_start_time = now.toString("YYYY-MM-dd HH:mm:ss.sss")
    val datePartition = now.toString("YYYY-MM-dd")
    val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.sss")
    val nowMillis = datetime_format.parseDateTime(batch_start_time).withZone(timeZone).getMillis
    val restartInd = propertyConfigs.getOrElse("spark.DataIngestion.restartabilityInd", "")
    val considerBatchWindow = if (hardDeleteBatch == "Y")
      'Y' else
      propertyConfigs.getOrElse("spark.DataIngestion.considerBatchWindow", "")
    val batchPartition = restartInd match {
      case "Y" => failedPartition
      case _ => nowMillis.toString
    }

    val (jdbcSqlConnStr, driver) = if (cacheInd == 'Y') {
      val sqlConnStr = JdbcConnectionUtility.constructTypeListJDBCConnStr(propertyConfigs)
      val jdbcDriver = JdbcConnectionUtility.getTLJDBCDriverName(propertyConfigs)
      (sqlConnStr, jdbcDriver)
    }
    else {
      val sqlConnStr = JdbcConnectionUtility.constructJDBCConnStr(propertyConfigs)
      val jdbcDriver = JdbcConnectionUtility.getJDBCDriverName(propertyConfigs)
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
    //val lookUpFile = propertyConfigs.getOrElse("spark.dataingestion.lookUpFile", "")
    //val tableFile: String = propertyConfigs.getOrElse("spark.dataingestion.tableFile", "")


    val cdcColFromTableSpecStr = tableSpecMap.getOrElse(tableName, "").split(splitString, -1)
    val (cdcColFromTableSpec, hardDeleteFlag) = if (cacheInd != 'Y') {
      //Holder.log.info("DataFrameLoader-tableName: " + tableName)
      //Holder.log.info("cdcColFromTableSpecStr: " + cdcColFromTableSpecStr.mkString)
      (cdcColFromTableSpecStr(0), cdcColFromTableSpecStr(1))
    } else ("", "")
    //val lookUpFile = propertyConfigs.getOrElse("spark.dataingestion.lookUpFile", "")
    //val metaInfoForLookupFile = new MetaInfo(lookUpFile)


    def parseQueryAndOptions =
      if (cacheInd == 'Y') {
        Success((s"(select * from $tableName ) $df_name", Map("" -> "")))
      } else {
        val (cdcQuery: String, cdcCols: String) = cdcQueryMap.getOrElse(df_name_arr(2), "").split("\\:") match {
          case Array(query, cdcField) => {
            //Holder.log.info("####Capturing cdcQueryMapInfo inside read Table: " + query.toString + " : " + cdcField.toString)
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
          val boundQuery = s"(${cdcQuery}) bounds"
         // Holder.log.info("####Capturing queryInfo inside read Table-Truncate: " + boundQuery)
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
          //Holder.log.info("####Capturing upperBound inside read Table-Truncate: " + conditionForQuery)

          def bounds: Try[org.apache.spark.sql.Row] = Try {
            spark.sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> boundQuery)).load.first
          }
          //  Holder.log.info("After Bound Query: " + tableName + ":" +  bounds.get)

          bounds match {
            case Success(row) => {
              def boundsMap = row.getValuesMap[AnyVal](row.schema.fieldNames)

              //    Holder.log.info("Bounds Map: " + boundsMap)
              def lower: AnyVal = Option(boundsMap.getOrElse("lower", 0L)) match {
                case None => 0L
                case Some(value) => value
              }
              def upper: AnyVal = Option(boundsMap.getOrElse("upper", 0L)) match {
                case None => 0L
                case Some(value) => value
              }

              def boundDifference = upper.asInstanceOf[Number].longValue() - lower.asInstanceOf[Number].longValue()
              /*if(upper.isInstanceOf[Date]) {
                Months.monthsBetween(new DateTime().withDate(new LocalDate(upper.asInstanceOf[Date])),
                  new DateTime().withDate(new LocalDate(upper.asInstanceOf[Date]))).asInstanceOf[Number].longValue()
              } else {
                upper.asInstanceOf[Number].longValue() - lower.asInstanceOf[Number].longValue()
              }*/



              def numPartitionStr =  {
                if(upper.isInstanceOf[Number])
                  if (boundDifference <= 1000000L) 1L else {
                    val part = (boundDifference / 5000000) * 4
                    if (part < 5L) 10L else if (part > 100L) 100L else part
                  }
                else 5L
              }

              val numPartitions = if (cdcColFromTableSpecStr(3) == "") numPartitionStr else cdcColFromTableSpecStr(3)
              val partitionMap: Map[String, String] = Map("partitionColumn" -> cdcCols, "lowerBound" -> lower.toString, "upperBound" -> upper.toString, "numPartitions" -> numPartitions.toString)
              //    Holder.log.info("####Capturing queryInfo inside read Table- Truncate: " + s"(select * from ${tableName}) ${df_name}")
              if (cdcColFromTableSpec.trim == "" || cdcColFromTableSpec.contains("truncate") || cdcColFromTableSpec.endsWith("id")) {
                Success(s"(select  cast(null as datetime) as deleteTime, $decryptCol * from ${tableName} ) ${df_name}", partitionMap)
              } else if (hardDeleteFlag == "cdc" && tableName.contains(".cdc.")) {
                Success(s"(select  ${cdcColFromTableSpec} deleteTime, $decryptCol * from ${tableName}  $conditionForQuery) ${df_name}", partitionMap)
              } else
                Success(s"(select  cast(null as datetime) as deleteTime, $decryptCol * from ${tableName} $conditionForQuery ) ${df_name}", partitionMap)
            }
            case Failure(exception) => {
              //   Holder.log.info("#####Captured ref table: " + refTableList + "-" + tableName)
              //TODO - Need to analyze why this step is required.
              if (refTableList.contains(tableName))
                Success(s"(select  cast(null as datetime) as deleteTime,$decryptCol * from ${tableName}) ${df_name}", Map("" -> ""))
              else
                Failure(new Throwable(exception.getMessage + splitString + boundQuery))
            }
          }
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
          if (refTableList.contains(tableName)) {
            val querytoExecute = s"(select cast(null as datetime) as deleteTime, $decryptCol * from ${tableName}) ${df_name}"
            Success(querytoExecute, partitionMap)
          }
          else if (cdcCols.endsWith("id") || cdcCols.endsWith("yyyymm") || cdcCols.startsWith("loadtime")) {
            /*val maxCdcDF: Try[org.apache.spark.sql.Row] = Try {
              spark.sqlContext.sql(cdcQuery).first
            }*/
            val maxCDF = if (PreviousBatchWindowEnd.trim.isEmpty || PreviousBatchWindowEnd == "1900-01-01" ) "0" else PreviousBatchWindowEnd
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
          Holder.log.info("The query to be executed : " + query)
        Success(spark.sqlContext.read.format("jdbc")
          .options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> query) ++ optns), query, numPartitions
        )
      }
      case Failure(ex) => Failure(ex)
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
              val df = if (refTableList.contains(tableName)) {
                //Holder.log.info("I am here loding df: ")
                dfReaderAndQuery._1.load()
              }
              else {
                dfReaderAndQuery._1.load()
                  .withColumn("ingestiondt", lit(datePartition))
                  .withColumn("batch", lit(batchPartition.toString))
              }

              val (min_window, max_window) = if (considerBatchWindow == "Y")
                (PreviousBatchWindowStart, PreviousBatchWindowEnd)
              else
                (null, null)

              val operationalFields = df.schema.fieldNames.filter(_.contains("$"))
                //Holder.log.info(df_name + ": " + df.count())
              if(loadType == "TL")
                  df.drop(operationalFields: _*).createOrReplaceTempView(df_name)
              else
                df.drop(operationalFields: _*).cache.createOrReplaceTempView(df_name)

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
        buildDeleteRecords(spark, tableName, propertyConfigs, cdcColFromTableSpecStr, datePartition, batchPartition.toString)
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

  def buildDeleteRecords(spark: SparkSession, tableToBeIngested: String, propertyConfigs: Map[String, String],
                         cdcColFromTableSpecStr: Array[String], datePartition: String, batchPartition: String) = {
    //val cdcColFromTableSpecStr = tableSpecMap.getOrElse(tableToBeIngested, "").split("-", -1)
    val partitionBy = cdcColFromTableSpecStr(2)
    val tableDF_arr = tableToBeIngested.split("\\.")
    val query = s"(select $partitionBy from $tableToBeIngested) ${tableDF_arr(2)}"
    Holder.log.info("delete query: " + query)
    val jdbcSqlConnStr = JdbcConnectionUtility.constructJDBCConnStr(propertyConfigs)
    val driver = JdbcConnectionUtility.getJDBCDriverName(propertyConfigs)
    val sourceDB = tableToBeIngested.split("\\.")(0).toLowerCase
    /*
     * EDIN-***: start Instead of hard coding the suffix of hive database now we are passing the whole DB name from property file
     */
    //val hiveDB = propertyConfigs.getOrElse("spark.DataIngestion.targetDB", "") + dbMap.getOrElse(sourceDB, "") + "_data_processed"
    val hiveDB = propertyConfigs.getOrElse("spark.DataIngestion.targetDB", "")
    //val hiveSecuredDB = propertyConfigs.getOrElse("spark.DataIngestion.targetSecuredDB", "") + dbMap.getOrElse(sourceDB, "") + "_data_processed"
    val hiveSecuredDB = propertyConfigs.getOrElse("spark.DataIngestion.targetSecuredDB", "")
    /*
     * EDIN-***: end Instead of hard coding the suffix of hive database now we are passing the whole DB name from property file
     */
    val formattedSourceDBName = if(tableDF_arr(0).contains("-"))
      tableDF_arr(0).replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_")
    else
      tableDF_arr(0)
    val viewName = formattedSourceDBName + "_" + tableDF_arr(2) + "_delete"

    def sourceDFwithIDs =
      spark.sqlContext.read.format("jdbc")
        .options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> query)).load
        .repartitionByRange(20, col(s"$partitionBy"))
        .withColumn("identifier", lit("dummy"))

    def targetDFwithIDs =
      spark.sql(s"select $partitionBy, cast(ingestiondt as String) , batch, deleted_flag from $hiveDB.${tableDF_arr(2)} ")
        .repartitionByRange(20, col(s"$partitionBy"))
    val deleteRecords = targetDFwithIDs.join(sourceDFwithIDs, Seq(partitionBy), "left")
                                       .groupBy(col(partitionBy))
                                       .agg(max(col("ingestiondt")).as("ingestiondt"),
                                         max(col("batch")).as("batch"),
                                         max(col("identifier")).as("identifier"),
                                         max(col("deleted_flag")).as("deleted_flag"))
                                       .filter(col("identifier").isNull)
                                       .filter(col("deleted_flag") === "0")
                                       .drop("identifier")
                                       .drop("deleted_flag")
    // .rdd.map(row => (row.getAs[Long](0),
                                        //                row.getAs[String](1),
                                        //row.getAs[String](2))).collect()

    //Holder.log.info("deleteRecords Size: " + deleteRecords.mkString("--"))

    /*val deleteRecordsID = deleteRecords.map(_._1).distinct.mkString(",")
    val batchPartitionList = deleteRecords.map(_._3).distinct.map('"' + _ + '"').mkString(",")
    val datePartitionList = deleteRecords.map(_._2).distinct.map('"' + _ + '"').mkString(",")*/
    val secureTableInd = spark.catalog.tableExists(s"$hiveSecuredDB.${tableDF_arr(2)}")
    val hiveTable = if (secureTableInd) s"$hiveSecuredDB.${tableDF_arr(2)}" else s"$hiveDB.${tableDF_arr(2)}"

    val deleteRecordsDF = if (!deleteRecords.head(1).isEmpty)
      spark.sql(s"select * from $hiveTable " ).
        join(broadcast(deleteRecords), Seq("ingestiondt", "batch", s"$partitionBy"),"inner")
        .withColumn("deleted_flag", lit(1))
        .withColumn("ingestiondt", lit(datePartition))
        .withColumn("batch", lit(batchPartition.toString))
    /*  )+
        s"where ingestiondt in ($datePartitionList) " +
        s"and batch in ($batchPartitionList) " +
        s"and $partitionBy in ($deleteRecordsID)")*/

    else
      spark.sql(s"select * from $hiveTable where 1 = 0")
    deleteRecordsDF.cache.createOrReplaceTempView(viewName)
    viewName
  }

  def buildRecords(considerBatchWindow: String, restartInd: String, hardDeleteFlag: String, tableName: String,
                   cdcColFromTableSpec: String, PreviousBatchWindowStart: String,
                   PreviousBatchWindowEnd: String, deleteRecordFilter: String,
                   refTableList: String, cdcCols: String, df_name: String) = {
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
    if (refTableList.contains(tableName)) {
      val querytoExecute = s"(select cast(null as datetime) as deleteTime, * from ${tableName}) ${df_name}"
      Success(querytoExecute, partitionMap)
    }
    else if (cdcCols.endsWith("id")) {
      /*val maxCdcDF: Try[org.apache.spark.sql.Row] = Try {
        spark.sqlContext.sql(cdcQuery).first
      }*/
      val maxCDF = if (PreviousBatchWindowEnd.trim.isEmpty) "0" else PreviousBatchWindowEnd
      //Holder.log.info("####Capturing queryInfo inside read Table - Incremental: " + s"(select * from ${tableName} where ${cdcCols} > ${Cdc} ) ${df_name}")
      val partitionMap: Map[String, String] = Map("" -> "")
      Success(s"(select cast(null as datetime) as deleteTime, * from ${tableName} where ${cdcCols} > '${maxCDF}' ) ${df_name}", partitionMap)
    }
    else if (hardDeleteFlag == "cdc" && tableName.contains(".cdc.")) {
      val queryToExecute = s"(select ${cdcCols} deleteTime, * from ${tableName} $conditionForQuery) ${df_name}"
      Success(queryToExecute, partitionMap)
    }
    else {
      val queryToExecute = s"(select cast(null as datetime) as deleteTime, * from ${tableName} $conditionForQuery ) ${df_name}"
      Success(queryToExecute, partitionMap)
    }
  }

}
