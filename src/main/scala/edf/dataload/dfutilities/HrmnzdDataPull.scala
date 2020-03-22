package edf.dataload.dfutilities

import edf.dataload.{auditDB, extractProcessName, loadFromStage,
  loadOnlyTLBatch, propertyMap, stageTablePrefix,
  initialLoadStagingDB, initialLoadSecuredStagingDB}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

object HrmnzdDataPull {
  def getTLDataFromHrmnzd(tableName: String, piiColList: List[String], cdcCol: String)
                         (implicit spark: SparkSession) = {
    val hrmnzdDB = piiColList match {
      case pii: List[String]
        if pii.isEmpty => loadFromStage match {
        case true =>  propertyMap.getOrElse ("spark.DataIngestion.targetStageDB", "")
        case false => propertyMap.getOrElse ("spark.DataIngestion.targetDB", "")
      }
      case _: List[String] => loadFromStage match {
        case true => propertyMap.getOrElse("spark.DataIngestion.targetSecuredStageDB", "")
        case false => propertyMap.getOrElse("spark.DataIngestion.targetSecuredDB", "")
      }
    }

    def maxTLBatch = spark.sql(s"select max(batch) from $auditDB.audit " +
      s"where processname = '$extractProcessName' and loadType = 'TL' and " +
      s"tablename like '%$tableName' ").first().getString(0)

    val df = if (loadFromStage) {
      //spark.sql(s"select * from ${hrmnzdDB.replaceAll("test_","")}.$stageTablePrefix$tableName")
      if(spark.catalog.
          tableExists(s"$initialLoadSecuredStagingDB.$stageTablePrefix$tableName"))
        spark.sql(s"select * from $initialLoadSecuredStagingDB.$stageTablePrefix$tableName")
      else
        spark.sql(s"select * from $initialLoadStagingDB.$stageTablePrefix$tableName")
    }
    else if (loadOnlyTLBatch) {
      spark.sql(s"select * from $hrmnzdDB.$tableName where " +
        s"batch = '$maxTLBatch' ")
    } else {
      val fullData = spark.sql(s"select * from $hrmnzdDB.$tableName")
      val sortCols = if (fullData.schema.fieldNames.contains("deleted_flag"))
        List("uniqueid") else List(cdcCol)
      if(cdcCol.endsWith("id"))
        fullData
      else
        getMostRecentRecords(fullData, List("id"), List("uniqueid", "batch"))
    }

    df
  }
  def getMostRecentRecords(df: DataFrame, partition_col: List[String],
                           sortCols: List[String]): DataFrame = {
    val part = Window.
      partitionBy(partition_col.head,partition_col:_*).
      orderBy(array(sortCols.head,sortCols:_*).desc)
    val dfWithRank = df.withColumn("rn", row_number().over(part))
    val dfWithDuplicatesRemoved = dfWithRank.
      filter("rn==1").
      drop("rn")
    dfWithDuplicatesRemoved
  }
}
