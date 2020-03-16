package edf.dataload.dfutilities

import edf.dataload.{hiveDB, hiveSecuredDB, s3Location, s3SecuredLocation, stageTablePrefix, stgLoadBatch}
import edf.dataload.dfactions.writeToS3
import edf.utilities.Holder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PiiData {
  def handlePiiData(df: DataFrame, hardDeleteDF: DataFrame, piiColList: List[String], tableToBeIngested: String,
                    batchPartition: String, cdcColMax: String, saveMode: SaveMode)
                   (implicit spark: SparkSession) = {
    val tableDF_arr = tableToBeIngested.split("\\.")
    val hiveTableName = if(stgLoadBatch)
      s"$hiveDB.$stageTablePrefix" + tableDF_arr(2)
    else
      s"$hiveDB." + tableDF_arr(2)
    val hiveSecureTable = if(stgLoadBatch)
      s"$hiveSecuredDB.$stageTablePrefix" + tableDF_arr(2)
    else
      s"$hiveSecuredDB." + tableDF_arr(2)

    def piiDataClassification(df: DataFrame, hardDeleteDF: DataFrame, piiColList: List[String]) : (Long, Long) = {
      piiColList match {
        case pii: List[String]
          if pii.isEmpty =>
          Holder.log.info("hiveDB: " + hiveDB + "-" + hiveTableName)
          writeToS3(df, hardDeleteDF, s3Location + tableDF_arr(2),
            hiveTableName, saveMode, batchPartition, cdcColMax)
        case pii: List[String] => {
          Holder.log.info("hiveDBSecure: " + hiveSecuredDB + "-" + hiveSecureTable)
          writeToS3(df, hardDeleteDF, s3SecuredLocation + tableDF_arr(2),
            hiveSecureTable, saveMode, batchPartition, cdcColMax)
          val dfFromS3 = spark.sql(s"select * from $hiveSecureTable where " +
            s"batch = '$batchPartition'")
          val dfUpdated: DataFrame = pii.foldLeft(dfFromS3)((d, c) => d.withColumn(c, lit("")))
          piiDataClassification(dfUpdated, hardDeleteDF, List.empty[String])
        }
      }
    }
    piiDataClassification(df, hardDeleteDF, piiColList)
  }

}
