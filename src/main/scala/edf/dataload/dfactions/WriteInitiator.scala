package edf.dataload.dfactions

import edf.dataload.{considerBatchWindowInd, hardDeleteBatch, metaInfoForLookupFile, propertyMap, splitString, tableGroup}
import edf.utilities.Holder
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object WriteInitiator {
  def s3WriteIterator(tableName: String, saveMode: SaveMode,
                      batchPartition: String, replicationTime: String,
                      currMinWindow: String, currMaxWindow: String,
                      prevWindow: (String, String), acc: Int, hardDeleteDF: DataFrame)
                     (implicit spark: SparkSession): Try[(Long, Long, String, String, Any, Any)] = {
    val considerBatchWindow = if(hardDeleteBatch == "Y") "Y" else considerBatchWindowInd
    Try {
      PrepareDataToWrite.writeDataFrametoS3(tableName, propertyMap, metaInfoForLookupFile,
        tableGroup, saveMode, batchPartition, replicationTime, hardDeleteBatch, hardDeleteDF)
    } match {
      case Success((srcCount, tgtCount, minWindow, maxWindow)) =>
        if (tgtCount == srcCount){
          //if consider batch window is ON, then use the data from dataframeLoader to capture the window timings.
          //Otherwise window timings will be calculated based on the updatetime minus 2 mins logic.
          val (minWindowStr, maxWindowStr) = considerBatchWindow match {
            case "Y" => (currMinWindow, currMaxWindow)
            case _ => (minWindow, if(srcCount == 0L) prevWindow._2 else maxWindow)
          }
          Holder.log.info(s"maxwindow: $tableName " + minWindowStr + ":" + maxWindowStr)
          Success((srcCount, tgtCount, "success", "",minWindowStr, maxWindowStr))
        }
        else
          acc match {
            case 0 => s3WriteIterator(tableName, saveMode, batchPartition, replicationTime,
                                      currMinWindow, currMaxWindow, prevWindow, 1, hardDeleteDF)
            case 1 =>
              val (minWindowStr, maxWindowStr) = considerBatchWindow match {
                case "Y" => (currMinWindow, currMaxWindow)
                case _ => (minWindow, maxWindow)
              }
              Failure(new Throwable(srcCount + splitString + tgtCount + splitString + "failed"
                + splitString + "Count Mismatch" + splitString + minWindowStr + splitString + maxWindowStr))
          }
      case Failure(ex) =>
        acc match {
          case 0 => s3WriteIterator(tableName, saveMode, batchPartition, replicationTime,
            currMinWindow, currMaxWindow,prevWindow, 1, hardDeleteDF)
          case 1 =>
            val (minWindowStr, maxWindowStr) = considerBatchWindow match {
              case "Y" => (currMinWindow, currMaxWindow)
              case _ => (prevWindow._1, prevWindow._2)
            }
            Failure(new Throwable(0L + splitString + 0L + splitString + "failedUnknown" +
              splitString + ex.getMessage + ex.getStackTrace.mkString("\n") + splitString + minWindowStr + splitString + maxWindowStr))
        }
    }
  }

  def apply(tableName: String, saveMode: SaveMode,
            batchPartition: String, replicationTime: String, currMinWindow: String,
            currMaxWindow: String, prevWindow: (String, String), acc: Int, hardDeleteDF: DataFrame)
           (implicit spark: SparkSession) = {
    s3WriteIterator(tableName, saveMode, batchPartition, replicationTime,
      currMinWindow, currMaxWindow, prevWindow, acc, hardDeleteDF)
  }
}
