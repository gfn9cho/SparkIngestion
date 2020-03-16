package edf.dataload.dfutilities

import edf.dataload.{incrementalpartition, isConnectDatabase, loadType,
  oldConsolidateTypeList, restartabilityInd, splitString, tableSpecMap, targetDB}
import edf.dataload.helperutilities.CdcColumnList
import edf.utilities.SqlConditionBuilder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map

object TypeTableJoins {
  def joinTypeTables(spark: SparkSession, mainTable: String, ref_col_list: List[String],
                     tableGroup: Map[String, List[String]], batchPartition: String) = {
    val typeTableList: List[String] = tableGroup.get(mainTable) match {
      case Some(tables: List[String]) => tables
      case None => Nil
    }
    val mainDF_arr = mainTable.split("\\.")
    val formattedSourceDBName = if(mainDF_arr(0).contains("-"))
      mainDF_arr(0).
        replaceAll("\\[", "").
        replaceAll("\\]", "").
        replaceAll("-", "_")
    else
      mainDF_arr(0)
    val mainDFStr = restartabilityInd match {
      case "Y" => formattedSourceDBName + "_" + mainDF_arr(2) + "_" + batchPartition
      case "N" => formattedSourceDBName + "_" + mainDF_arr(2)
    }

    val tableSpecMapStr = tableSpecMap.getOrElse(mainTable, "").split(splitString, -1)
    val hardDeleteFlag = tableSpecMapStr(1)
    val cdcCol = CdcColumnList.getCdcColMax(mainTable)
    val bucketColumn = if(cdcCol._2.endsWith("id") ||
      cdcCol._2.endsWith("yyyymm") ||
      cdcCol._2.startsWith("loadtime"))
      col("ingestiondt")
    else
      cdcCol._1
    val partitionByCol = col(cdcCol._3)
    val cdcDF = mainDF_arr(0) + "_dbo_" + mainDF_arr(2) + "_CT"
    val mainDFBeforeRepartition = spark.sql(s"select * from $mainDFStr")
      .withColumn("ingestiondt", date_format(
        bucketColumn, "YYYY-MM-dd"))
      .withColumn("uniqueId", concat(
        bucketColumn.cast("Long"),partitionByCol))
    val numPart = if(loadType == "TL")
      mainDFBeforeRepartition.
        select(trunc(col("ingestiondt"),"MM").as("months")).
        dropDuplicates.count.toInt * 2
    else
      incrementalpartition.toInt
    //Holder.log.info("NumPartitions: " + numPart + "-" + bucketColumn)
    /*
     * EDIN-362: Create empty hive table in harmonized layer for TL
     */
    val mainDF = if(numPart > 0)
      mainDFBeforeRepartition
        .repartition(numPart,col("ingestiondt"))
        .withColumn("ingestiondt", trunc(date_format(
          bucketColumn, "YYYY-MM-dd"),"MM").cast("String"))
    else
      mainDFBeforeRepartition
        .withColumn("ingestiondt", trunc(date_format(
          bucketColumn, "YYYY-MM-dd"),"MM").cast("String"))
    val srcDF = if (hardDeleteFlag == "cdc")
      mainDF
        .withColumn("deleted_flag", lit(0))
        .union(spark.sql(s"select * from $cdcDF")
          .withColumn("deleted_flag", lit(1)))
    else if(hardDeleteFlag == "id")
      mainDF.withColumn("deleted_flag", lit(0))
    else
      mainDF

    def makeJoins(df: DataFrame, typeTableList: List[(String, Int)]): DataFrame = {
      typeTableList match {
        case Nil => df
        case types :: tail => {
          val typeTableSqlSplit = types._1.split(splitString, -1)
          val typeTable_arr = typeTableSqlSplit(0).split("\\.")
          val formattedTypeDBName = if(typeTable_arr(0).contains("-"))
            typeTable_arr(0).replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("-", "_")
          else
            typeTable_arr(0)
          val typeTable = formattedTypeDBName + "_" + typeTable_arr(2)
          val sqlStr = typeTableSqlSplit(1)
          val typeDF = if(isConnectDatabase) {
            val typeData = spark.table(oldConsolidateTypeList).
              filter(col("typelistName") === typeTableSqlSplit(0))
            if(!typeData.head(1).isEmpty)
              typeData
            else
              spark.sql(s"select * from $targetDB.${typeTable_arr(2)}")
          } else
            spark.sql(s"select * from $typeTable")
          val condition = SqlConditionBuilder.parseCondition(types._2, sqlStr, ref_col_list)
          val selfJoinInd = if (mainDF == typeTable) 'Y' else 'N'
          val joinedDF = JoinDataFrames.joinDFs(df, typeDF, condition, "left", selfJoinInd)

          makeJoins(joinedDF, tail)
        }
      }
    }

    makeJoins(srcDF, typeTableList.zipWithIndex)
  }
}
