package edf.dataload.dfutilities

import edf.dataload.{hiveDB, hiveSecuredDB, propertyMap, stageTablePrefix, stgLoadBatch}
import edf.dataload.helperutilities.PartitionBounds
import edf.utilities.{Holder, JdbcConnectionUtility}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, lit}

object BuildDeleteRecords {
  def buildDeleteRecords( tableToBeIngested: String, cdcColFromTableSpecStr: Array[String],
                          datePartition: String, batchPartition: String)
                        (implicit spark: SparkSession)= {
    //val cdcColFromTableSpecStr = tableSpecMap.getOrElse(tableToBeIngested, "").split("-", -1)
    val partitionBy = cdcColFromTableSpecStr(2)
    val tableDF_arr = tableToBeIngested.split("\\.")
    val query = s"(select $partitionBy from $tableToBeIngested) ${tableDF_arr(2)}"
    Holder.log.info("delete query: " + query)
    val jdbcSqlConnStr = JdbcConnectionUtility.constructJDBCConnStr(propertyMap)
    val driver = JdbcConnectionUtility.getJDBCDriverName(propertyMap)
    val boundQuery = s"(select min(id) as lower, max(id) as upper from $tableToBeIngested ) bounds"
    def partitionOptions = PartitionBounds(tableDF_arr(2), boundQuery, cdcColFromTableSpecStr)

    val formattedSourceDBName = if(tableDF_arr(0).contains("-"))
      tableDF_arr(0).replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_")
    else
      tableDF_arr(0)
    val viewName = formattedSourceDBName + "_" + tableDF_arr(2) + "_delete"

    def sourceDFwithIDs =
      spark.sqlContext.read.format("jdbc")
        .options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> query) ++ partitionOptions).load
        //.repartitionByRange(50, col(s"$partitionBy"))
        .withColumn("identifier", lit("dummy"))

    val targetSql = if (stgLoadBatch)
      s"select $partitionBy from $hiveDB.$stageTablePrefix${tableDF_arr(2)} "
    else
      s"select $partitionBy, cast(ingestiondt as String) , batch, deleted_flag " +
        s"from $hiveDB.${tableDF_arr(2)} "
    def targetDFwithIDs =
      spark.sql(targetSql)
       // .repartitionByRange(50, col(s"$partitionBy"))
    import org.apache.spark.sql.functions.{col, max}
    val deleteRecords = if(stgLoadBatch) {
      targetDFwithIDs.join(sourceDFwithIDs,
        Seq(partitionBy), "left").
        filter(col("identifier").isNull).
        drop("identifier")
    } else {
      targetDFwithIDs.join(sourceDFwithIDs, Seq(partitionBy), "left")
        .groupBy(col(partitionBy))
        .agg(max(col("ingestiondt")).as("ingestiondt"),
          max(col("batch")).as("batch"),
          max(col("identifier")).as("identifier"),
          max(col("deleted_flag")).as("deleted_flag"))
        .filter(col("identifier").isNull)
        .filter(col("deleted_flag") === "0")
        .drop("identifier")
        .drop("deleted_flag")
    }
    def secureTableInd = spark.catalog.tableExists(s"$hiveSecuredDB.${tableDF_arr(2)}")
    def hiveTable = if(stgLoadBatch)
      s"$hiveDB.$stageTablePrefix${tableDF_arr(2)}"
    else if (secureTableInd)
      s"$hiveSecuredDB.${tableDF_arr(2)}"
    else
      s"$hiveDB.${tableDF_arr(2)}"

    def deleteRecordsDF = if (!deleteRecords.cache.head(1).isEmpty && !stgLoadBatch)
      spark.sql(s"select * from $hiveTable " ).
        join(broadcast(deleteRecords), Seq("ingestiondt", "batch", s"$partitionBy"),"inner")
        .withColumn("deleted_flag", lit(1))
        .withColumn("ingestiondt", lit(datePartition))
        .withColumn("batch", lit(batchPartition.toString))
    else
      spark.sql(s"select * from $hiveTable where 1 = 0")

    /*if(stgLoadBatch) {
      deleteRecords.cache.createOrReplaceTempView(viewName)
    } else {
      deleteRecordsDF.cache.createOrReplaceTempView(viewName)
    }*/
    deleteRecordsDF.createOrReplaceTempView(viewName)
    viewName
  }

  def apply( tableToBeIngested: String, cdcColFromTableSpecStr: Array[String],
             datePartition: String, batchPartition: String="99999999999")
           (implicit spark: SparkSession) = {
    buildDeleteRecords(tableToBeIngested, cdcColFromTableSpecStr, datePartition, batchPartition)
  }
}
