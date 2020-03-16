package edf.dataload.dfutilities

import edf.dataload.loadType
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object EmptyDataFrameCheck {
  def emptyDataFrameInd(tableName: String, mainDF: String)(implicit spark: SparkSession) =
    if (tableName.startsWith("dbo_")) false
    else
    if (spark.catalog.tableExists(mainDF))
      Try {
        if (loadType == "TL")
          spark.table(mainDF).head(1).isEmpty
        else
          spark.table(mainDF).cache.head(1).isEmpty
      } match {
        case Success(bool) => bool
        case Failure(ex) =>
          throw new Exception(s"$mainDF failed with exception: ${ex.getLocalizedMessage}")
      }
    else false

  def apply(tableName: String, mainDF: String)(implicit spark: SparkSession) = {
    emptyDataFrameInd(tableName, mainDF)
  }
}
