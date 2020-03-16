package edf.dataload.helperutilities

import java.sql.Timestamp

import edf.dataload.{auditDB, considerBatchWindowInd, loadType,
  processName, propertyMap, dbType, now}
import edf.utilities.JdbcConnectionUtility
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

object ReplicationTime {
  def getReplTime()(implicit spark: SparkSession): String = {
    //val considerBatchWindow = propertyConfigs.getOrElse("spark.DataIngestion.considerBatchWindow", "")
    //val dateNow = now.minusMillis(2)
    //dateNow.add(Calendar.MILLISECOND, -2)
    val replTimeNow = if(dbType == "mysql") now.toString("YYYY-MM-dd HH:mm:ss.sss")
    else getUpdateTime(spark, propertyMap)
    if (replTimeNow <= getBatchWindowStartTime && considerBatchWindowInd == "Y") {
      Thread.sleep(30000)
      getReplTime
    }
    replTimeNow
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
  def getBatchWindowStartTime()(implicit spark: SparkSession) = if (loadType == "TL") "1900-01-01"
  else {
    def batchStart = spark.sql(s"select max(batchwindowend) from $auditDB.batchStats where processname = '${processName}' and harddeletebatch != 'Y'")

    batchStart.first().getAs[String](0)
  }

  def apply()(implicit spark: SparkSession) = {
    getReplTime()
  }
}
