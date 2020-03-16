package edf.missingdataload

import edf.dataingestion.{splitString, tableSpecMapTrimmed}
import org.apache.spark.sql.SparkSession
import edf.dataingestion._
import edf.utilities.Holder

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

object DraftQuery {
  def draftSrcQueryWithPartitions(tableToBeIngested: String)
                                 (implicit spark: SparkSession):
  (String, String, Map[String, String]) = {
    val tableDF_arr = tableToBeIngested.split("\\.")
    val table = tableDF_arr(2)
    // 1. for each table in missing record list identify the bound parameters
    val (numPartitionStr, cdcCols, lower, upper) = CalcPartitionBounds.getPartitionBoundValues(table)
    val cdcColFromTableSpecStr = tableSpecMapTrimmed.getOrElse(table, "").split(splitString, -1)

    val numPartitions = cdcColFromTableSpecStr.lift(3) match {
      case Some("") => numPartitionStr
      case Some(x) => x
      case None => numPartitionStr
    }

    val partitionMap: Map[String, String] = Map("partitionColumn" -> cdcCols, "lowerBound" -> lower.toString, "upperBound" -> upper.toString, "numPartitions" -> numPartitions.toString)

      Holder.log.info("cdcColFromTableSpecStr: " + cdcColFromTableSpecStr.mkString("-") + ":" + table)
    val partitionByCol =  Try{ cdcColFromTableSpecStr(2).trim } match {
      case Success(value) => value
      case Failure(exception) => Holder.log.info("cdccol exception:" + table + ":" + exception)
    }
    val cdcCol = cdcColFromTableSpecStr(0).trim

    val bbcuTableList = List("pc_policy","pc_account","pc_job","pc_policyperiod","pc_activity")
    val tablewithWhereClause = if (bbcuTableList.contains(table)) {
      s"$table where retired=0 "
    } else s"$table"

    val queryToExecute = if (partitionByCol == cdcCol)
      s"(select $partitionByCol from ${tablewithWhereClause} ) ${table}"
    else
      s"(select $partitionByCol, $cdcCol from ${tablewithWhereClause} ) ${table}"

    (table, queryToExecute, partitionMap)
  }

  def apply(tableName: String)(implicit spark: SparkSession) = {
      draftSrcQueryWithPartitions(tableName)
  }
}
