package edf.dataload.helperutilities

import edf.dataload.{cdcQueryMap, jdbcSqlConnStr, driver}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object PartitionBounds {
  def getBoundForPartition (tableName: String, boundQuery: String,
                            cdcColFromTableSpecStr: Array[String])(implicit spark: SparkSession ) = {
    val (cdcQuery: String, cdcCols: String) = cdcQueryMap.getOrElse(tableName, "")
      .split("\\:") match {
      case Array(query, cdcField) => {
        //Holder.log.info("####Capturing cdcQueryMapInfo inside read Table: " + query.toString + " : " + cdcField.toString)
        (query.toString, cdcField.toString)
      }
      case _ => ("", "")
    }

    def bounds: Try[org.apache.spark.sql.Row] = Try {
      spark.sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> boundQuery)).load.first
    }
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
        def numPartitionStr =  {
          if(upper.isInstanceOf[Number])
            if (boundDifference <= 1000000L) 1L else {
              val part = (boundDifference / 50000000) * 4
              if (part < 5L) 10L else if (part > 100L) 100L else part
            }
          else 5L
        }

        val numPartitions = if (cdcColFromTableSpecStr(3) == "") numPartitionStr else cdcColFromTableSpecStr(3)
        val partitionMap: Map[String, String] = Map("partitionColumn" -> "id", "lowerBound" -> lower.toString, "upperBound" -> upper.toString, "numPartitions" -> numPartitions.toString)
        partitionMap
      }
      case Failure(_) => Map("" -> "")

    }
  }
  def apply(tableName: String, boundQuery: String,
            cdcColFromTableSpecStr: Array[String])(implicit spark: SparkSession ) = {
    getBoundForPartition(tableName, boundQuery, cdcColFromTableSpecStr)
  }
}
