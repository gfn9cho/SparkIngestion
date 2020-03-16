package edf.missingdataload

import edf.dataingestion.cdcQueryMap
import edf.utilities.Holder
import org.apache.spark.sql.SparkSession
import edf.dataingestion._
import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

object CalcPartitionBounds {
  def getPartitionBoundValues(table: String)(implicit spark: SparkSession): (Any, String, Any, Any) = {
    cdcQueryMap.get(table) match {
      case Some(queryString) => {
        val str = queryString.split("\\:")
        val (cdcQuery: String, cdcCols: String) = (str(0), str(1))
        val boundQuery = s"($cdcQuery) bounds"
        Holder.log.info("boundQueryyyyy: " + boundQuery)
        Holder.log.info("cdcColsssss: " + cdcCols)


        def bounds: Try[org.apache.spark.sql.Row] = Try {
          spark.sqlContext.read.format("jdbc").
            options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> boundQuery)).load.first
        }

        bounds match {
          case Success(row) => {
            def boundsMap = row.getValuesMap[AnyVal](row.schema.fieldNames)

            def lower: AnyVal = Option(boundsMap.getOrElse("lower", 0L)) match {
              case None => 0L
              case Some(value) => value
            }

            def upper: AnyVal = Option(boundsMap.getOrElse("upper", 0L)) match {
              case None => 0L
              case Some(value) => value
            }

            def boundDifference = upper.asInstanceOf[Number].longValue() - lower.asInstanceOf[Number].longValue()

            def numPartitionStr = {
              if (upper.isInstanceOf[Number]) {
                if (boundDifference <= 1000000L) 1L else {
                  val part = (boundDifference / 10000000) * 4
                  if (part < 5L) 10L else if (part > 100L) 100L else part
                }
              } else 1L
            }

            (numPartitionStr, cdcCols, lower, upper)
          }
          //TODO implement failure scenario
          case Failure(ex) => ("", "", "", "")
        }
      }
      case None => ("", "", "", "")
    }
  }

  def apply(table: String)(implicit spark: SparkSession) = {
    getPartitionBoundValues(table)
  }
}
