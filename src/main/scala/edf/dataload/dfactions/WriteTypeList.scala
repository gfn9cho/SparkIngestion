package edf.dataload.dfactions

import edf.dataload._
import edf.utilities.{BatchConcurrency, Holder}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object WriteTypeList {

  def writeConnectTypeListTables(spark: SparkSession, propertyConfigs: Map[String, String],
                                 TypeListIngestionFutures: Iterator[Future[Try[String]]],
                                 mainTableListFromTableSpec: List[String]) = {


    spark.catalog.tableExists(s"$auditDB." + oldConsolidateTypeList) match {
      case true =>
        spark.catalog.refreshTable(s"$auditDB.$oldConsolidateTypeList")
        spark.sql(s"select * from $auditDB.$oldConsolidateTypeList")
        .coalesce(1)
        .createOrReplaceTempView(oldConsolidateTypeList)
        mergeDFS('Y')
      case false => mergeDFS('N')
    }

    def mergeDFS(compareInd: Char) = {
      Holder.log.info("Inside MergeDF")

      val typeTableResults = BatchConcurrency.awaitSliding(TypeListIngestionFutures,50).map {
        case Success(value) => value.split(splitString)
        case Failure(exception) => exception.getMessage.split(splitString)
      }.toList
      val typeTableSucceeded = typeTableResults.filter(_ (6) == "success").map(_(2))
      val refTableList = typeTableSucceeded.filter(!mainTableListFromTableSpec.contains(_))
      val mergedDfFuture = refTableList.toIterator.map(typetable => BatchConcurrency.executeAsync{
        val tableStr = typetable.split("\\.")
        val tableDF =if(tableStr(0).contains("-"))
          tableStr(0).replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_") + "_" + tableStr(2)
          else
          tableStr(0) + "_" + tableStr(2)
        spark.sql(s"select * from $tableDF").withColumn("typelistName", lit(typetable))
      })

      val mergedDf = BatchConcurrency.awaitSliding(mergedDfFuture,50).reduce(_.union(_)).coalesce(1)


      mergedDf.cache.createOrReplaceTempView(newConsolidateTypeList)


      if (compareInd == 'Y') {
      //     Holder.log.info("typenewvaluesdf1: " + spark.table("newValuesDF").count)
     //   if (!spark.table("newValuesDF").head(1).isEmpty) {
        mergedDf.
          except(spark.sql(s"select * from $oldConsolidateTypeList")).
          createOrReplaceTempView("newValuesDF")
        spark.table("newValuesDF").cache()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","static")
        getTypeListTables(typeTableResults.toIterator, spark.table("newValuesDF"))
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", partitionOverwriteMode)
          mergedDf.write.format("parquet")
            .mode(SaveMode.Overwrite)
            .options(Map("path" -> (auditPath + "/typelist/" + targetDB)))
            .saveAsTable(s"$auditDB." + oldConsolidateTypeList)

      //  }
      } else {
        mergedDf.write.format("parquet")
          .mode(SaveMode.Overwrite)
          .options(Map("path" -> (auditPath + "/typelist/" + targetDB)))
          .saveAsTable(s"$auditDB." + oldConsolidateTypeList)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","static")
        getTypeListTables(typeTableResults.toIterator, mergedDf)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", partitionOverwriteMode)
      }
      spark.catalog.refreshTable(s"$auditDB.$oldConsolidateTypeList")
      spark.sql(s"select * from $auditDB.$oldConsolidateTypeList").cache.createOrReplaceTempView(oldConsolidateTypeList)
      Holder.log.info("Exiting MergeDF")
    }

    def getTypeListTables(typeResultsIterator: Iterator[Array[String]], newTypes: DataFrame) = {
      val typeListToBeIngested = newTypes.select(col("typelistName"))
        .dropDuplicates.collect
        .map(_.getAs[String](0))

      val typeTableResults = typeResultsIterator.map { auditValues => BatchConcurrency.executeAsync{
        val writeFutures: Row = if (typeListToBeIngested.contains(auditValues(2)) && auditValues(6) == "success") {
          val sourceDBStr = auditValues(2).split("\\.")
          val sourceDB = sourceDBStr(0).toLowerCase
          /*
           * EDIN-***: start Instead of hard coding the suffix of hive database now we are passing the whole DB name from property file
           */
          //val hiveDB = targetDB + dbMap.getOrElse(sourceDB, "") + "_data_processed"
          val hiveDB = targetDB
          /*
           * EDIN-***: start Instead of hard coding the suffix of hive database now we are passing the whole DB name from property file
           */
          val hiveTableName = s"$hiveDB." + sourceDBStr(2)
          val harmonizedPath = hrmnzds3Path + "/" + sourceDBStr(2)

          val df = newTypes.where(col("typelistName")===auditValues(2))
          val sourceCount = df.count
          if (!mainTableListFromTableSpec.contains(auditValues(2))) {
            Try {
              df.write.format("parquet").options(Map("path" -> harmonizedPath))
                .mode(SaveMode.Append).saveAsTable(hiveTableName)
            } match {
              case Success(_) => None
              case Failure(ex) => {
              throw new Exception(ex.getMessage)
              }
            }
          }
          Row(auditValues(0), auditValues(1), auditValues(2), sourceCount, auditValues(4), auditValues(5), auditValues(6), "", auditValues(8), auditValues(9), null, null, auditValues(12), sourceCount,null)
        } else
          Row(auditValues(0), auditValues(1), auditValues(2), auditValues(3).toLong, auditValues(4), auditValues(5), auditValues(6), "", auditValues(8), auditValues(9), null, null, auditValues(12), auditValues(3).toLong,null)
        writeFutures
      } }


      val auditRowsIter = BatchConcurrency.awaitSliding[Row](typeTableResults,50)
      val auditRows = iterAction(auditRowsIter)

      val auditFrame = spark.createDataFrame(spark.sparkContext.parallelize(auditRows), schema)

      val auditSaveMode = if(spark.catalog.tableExists(s"$auditDB.audit"))
                          SaveMode.Append else SaveMode.Overwrite
      auditFrame
        .withColumn("processName", lit(processName))
        .write.format("parquet")
        .partitionBy("processName", "ingestiondt")
        .options(Map("path" -> (auditPath + "/audit")))
        .mode(auditSaveMode).saveAsTable(s"$auditDB.audit")
      Holder.log.info("Exiting writeConnect TypeList")
    }
  }

  def writeOtherTYpeListTables(spark: SparkSession,propertyConfigs: Map[String, String],
                               TypeListIngestionFutures: Iterator[Future[Try[String]]],
                               mainTableListFromTableSpec: List[String]) = {
    val auditPath = propertyConfigs.getOrElse("spark.DataIngestion.auditPath", "")
    val processName = propertyConfigs.getOrElse("spark.DataIngestion.processName", "")
    val auditDB = propertyConfigs.getOrElse("spark.DataIngestion.auditDB", "")


    def audit = BatchConcurrency.awaitSliding(TypeListIngestionFutures).map { auditValue =>
      val writeFutures: Future[Row] = BatchConcurrency.executeAsync(auditValue match {
        case Success(value) => {
          val auditValues = value.split(splitString)
          val sourceDBStr = auditValues(2).split("\\.")
          val sourceDB = sourceDBStr(0).toLowerCase.replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_")
          /*
          * EDIN-***: Start Instead of hard coding the suffix of hive database now we are passing the whole DB name from property file
          */
          //val hiveDB = targetDB + dbMap.getOrElse(sourceDB, "") + "_data_processed"
          val hiveDB = targetDB
          /*
           * EDIN-***: End Instead of hard coding the suffix of hive database now we are passing the whole DB name from property file
           */

          val tableDf = sourceDBStr(0).replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_") + "_" + sourceDBStr(2)
          val hiveTableName = s"$hiveDB." + sourceDBStr(2)
          val harmonizedPath = hrmnzds3Path + "/" + sourceDBStr(2)
      val df = spark.sql(s"select * from ${tableDf}")
          def cols = df.schema.fieldNames.map(
            colName => col(colName).as(colName.replaceAll("[\\.\\ ]","")))
          df.select(cols: _*).createOrReplaceTempView(tableDf)

          val sourceCount = df.count
          if (!mainTableListFromTableSpec.contains(auditValues(2))) {
            spark.table(tableDf).write.format("parquet").options(Map("path" -> harmonizedPath))
              .mode(SaveMode.Overwrite).saveAsTable(hiveTableName)
          }
          Row(auditValues(0), auditValues(1), auditValues(2), sourceCount, auditValues(4), auditValues(5), auditValues(6), "", auditValues(8), auditValues(9), null, null, auditValues(12), sourceCount,null)
      }
        case Failure(exception) => {
          val excptnValues = exception.getMessage.split(splitString, -1)
          Row(excptnValues(0), excptnValues(1), excptnValues(2), excptnValues(3).toLong, excptnValues(4), excptnValues(5), excptnValues(6), "", excptnValues(8), excptnValues(9), null, null, excptnValues(12), excptnValues(3).toLong,null)
        }
      })
      writeFutures
    }

    val auditRows = BatchConcurrency.awaitSliding[Row](audit,20)


    val auditFrame = spark.createDataFrame(spark.sparkContext.parallelize(auditRows.toList), schema)
    auditFrame
      .withColumn("processName", lit(processName))
      .write.format("parquet")
      .partitionBy("processName", "ingestiondt")
      .options(Map("path" -> (auditPath + "/audit")))
      .mode(SaveMode.Append).saveAsTable(s"$auditDB.audit")
    }
  }
