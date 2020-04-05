package edf.recon

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, col, when}
import edf.dataload._
import edf.utilities.Holder
import org.joda.time.format.DateTimeFormat

object gwRecon {
  def gwRecon(srcQuery: String, lakeQuery: String, src_sstm_cd: String)
             (implicit spark: SparkSession) = {

    Holder.log.info("Loading the data from sql server to spark dataframe")
    val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")
    val nowString = datetime_format.withZone(timeZone).
      parseDateTime(now.toString("YYYY-MM-dd HH:mm:ss.sss"))

    val maxCreateTimetgt = nowString.withTimeAtStartOfDay().
                              minusMillis(1).toString("YYYY-MM-dd HH:mm:ss.SSS")
    Holder.log.info(s"maxCreateTimetgt: $maxCreateTimetgt")
    Holder.log.info("invoking getSQLserverData function to get the dataframe,result_db and s3_path")
    Holder.log.info(s"SrcQuery: $srcQuery")
    Holder.log.info(s"LakeQuery: $lakeQuery")

    val sourceDF = spark.read.format("jdbc").
      options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> srcQuery)).load
    val lakeDF = spark.sql(lakeQuery)

    val srcDf = sourceDF.
      select(col("extractdate"),col("polstatus"),col("auditentity"),col("auditfrom"),
        col("auditthru"),col("auditresult").as("src_value")).
      withColumn("source_system_cd", lit(src_sstm_cd))
    val dlDf = lakeDF.
      select(col("auditentity"),col("auditresult").as("lake_value"))

    val reconDF = srcDf.join(dlDf, Seq("auditentity")).
      withColumn("recon_status", when(col("src_value")===col("lake_value"),
        lit("success")).otherwise(lit("failed"))).cache

    val saveMode =
      if (spark.catalog.tableExists(s"$reconResultDB.stg_recon"))
      SaveMode.Append else SaveMode.Overwrite
    reconDF.coalesce(1).write.format("parquet").
      partitionBy("source_system_cd").
      mode(saveMode).
      options(Map("path" -> s"$reconResultPath/stg_recon")).
      saveAsTable(s"$reconResultDB.stg_recon")
    reconDF
  }

  def apply(srcQuery: String, lakeQuery: String, src_sstm_cd: String)(implicit spark: SparkSession) = {
    gwRecon(srcQuery, lakeQuery, src_sstm_cd)
  }
}
