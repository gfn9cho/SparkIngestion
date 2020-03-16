package edf.dataload.dfactions

import edf.dataload.{deleteTableList, loadType, restartabilityInd, stgLoadBatch}
import edf.dataload.dfutilities.ReadTable
import edf.dataload.helperutilities.DefineCdcCutOffValues
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object CreateDataFrame {
  def createDF(tableInfo: (String, (String, String, String, String)),
               Indicator: Char, batch_window_start: String)
              (implicit spark: SparkSession):
  ((String,String),(String, (String, String, String, String)), DataFrame) = {
    val auditInfo = tableInfo._2
    val window_end_str = auditInfo._4
    val window_end = if (window_end_str == null) "1900-01-01" else window_end_str
    val tableDF_arr = tableInfo._1.split("\\.")
    case class Container[+T](element: T) {
      def get[T]: T = {
        element.asInstanceOf[T]
      }
    }
    val isDeleteTable =  if (loadType != "RB")
      deleteTableList.map(_.split("\\.")(2)).
        contains(tableDF_arr(2))
    else false
   val failedPartition =  if(restartabilityInd == "Y") auditInfo._2 else ""
    val dfActions = Array("read", "delete").par.map {
      case dest if dest == "read" =>
        (dest , Container[Try[String]](ReadTable( tableInfo._1,
        Indicator, batch_window_start, auditInfo._3, window_end, failedPartition)))
      case dest if dest == "delete" =>
        if(stgLoadBatch && isDeleteTable)
          (dest , Container[DataFrame](StageIncremental.getHardDeletesFromHrmnzd(tableDF_arr(2))))
        else
          (dest , Container[DataFrame](spark.emptyDataFrame))
    }.toMap

    (dfActions.apply("read").get[Try[String]],
      dfActions.apply("delete").get[DataFrame] ) match {
      case (Success(value), df: DataFrame) => {
        (("success", value),tableInfo, df)
      }
      case (Failure(exception), df:DataFrame) => {
        (("failed", exception.getMessage),tableInfo, df)
      }
      case _ => throw new Exception(s"Error in reading dataframe for table: ${tableDF_arr(2)}")
    }
    /*ReadTable( tableInfo._1,
      Indicator, batch_window_start, auditInfo._3, window_end, failedPartition) match {
      case Success(value) => {
        ((("success", value),tableInfo))
      }
      case Failure(exception) => {
        ((("failed", exception.getMessage),tableInfo))
      }
    }*/

  }

  def apply(tableInfo: (String, (String, String, String, String)),
            Indicator: Char, batch_window_start: String)
           (implicit spark: SparkSession) = {
    createDF(tableInfo, Indicator, batch_window_start)
  }
}
