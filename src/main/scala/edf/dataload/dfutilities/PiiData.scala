package edf.dataload.dfutilities

import edf.dataload.dfactions.writeToS3
import edf.dataload.{hiveDB, hiveSecuredDB, s3Location, s3SecuredLocation, schemaCheck, stageTablePrefix, stgLoadBatch}
import edf.utilities.Holder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PiiData {
  def handlePiiData(df: DataFrame, hardDeleteDF: DataFrame, piiColList: List[String], tableToBeIngested: String,
                    batchPartition: String, cdcColMax: String, saveMode: SaveMode, tableLoadType: String)
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
         val dfaltered = if(schemaCheck)
                                AlterSchema(df, hiveTableName, "", s3Location + tableDF_arr(2))
                          else df
          writeToS3(dfaltered, hardDeleteDF, s3Location + tableDF_arr(2),
            hiveTableName, saveMode, batchPartition, cdcColMax, tableLoadType)
        case pii: List[String] => {
          val dfaltered = if(schemaCheck)
                                AlterSchema(df, hiveTableName, hiveSecuredDB,s3SecuredLocation + tableDF_arr(2))
                          else df
          writeToS3(dfaltered, hardDeleteDF, s3SecuredLocation + tableDF_arr(2),
            hiveSecureTable, saveMode, batchPartition, cdcColMax, tableLoadType)
          val dfUpdated: DataFrame = pii.foldLeft(df)((d, c) => d.withColumn(c, lit("")))
          piiDataClassification(dfUpdated, hardDeleteDF, List.empty[String])
        }
      }
    }
    piiDataClassification(df, hardDeleteDF, piiColList)
  }

}
