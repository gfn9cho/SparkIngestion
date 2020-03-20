package edf.dataload.dfutilities

import edf.dataload.stgLoadBatch
import edf.utilities.Holder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object AlterSchema {
  def alterSchema(dftos3: Dataset[Row], hrmnzdHiveTableName: String,
                  hiveSecureDB: String)(implicit spark: SparkSession) = {
    val sourceSchema = dftos3.
      drop("ingestiondt").
      drop("bucket").
      schema.fields.map(field => field.name + " " + field.dataType.typeName)
    val targetSchema = spark.sql(s"select * from $hrmnzdHiveTableName").
                            drop("ingestiondt").
                            drop("bucket").
                            schema.fields.
                            map(field => field.name + " " + field.dataType.typeName)
    val newFields = sourceSchema.map(_.toLowerCase) diff targetSchema.map(_.toLowerCase)
    val dropFields = targetSchema.map(_.toLowerCase) diff sourceSchema.map(_.toLowerCase)
     if(newFields.size > 0 ) {
      val fieldsToAdd = sourceSchema.filter(field => newFields.map(_.toLowerCase).contains(field.toLowerCase)).mkString(",")
      if (hiveSecureDB != "") {
        val hiveTableName = hrmnzdHiveTableName.split("\\.")(1)
        val secureHiveTableName = s"$hiveSecureDB.$hiveTableName"
        if (spark.catalog.tableExists(s"$secureHiveTableName")) {
          spark.sql(s"ALTER TABLE $secureHiveTableName set tblproperties('external'='false')")
          spark.sql(s"ALTER TABLE $secureHiveTableName ADD COLUMNS ($fieldsToAdd)")
          spark.sql(s"ALTER TABLE $secureHiveTableName set tblproperties('external'='true')")
        }
      } else {
        spark.sql(s"ALTER TABLE $hrmnzdHiveTableName set tblproperties('external'='false')")
        spark.sql(s"ALTER TABLE $hrmnzdHiveTableName ADD COLUMNS ($fieldsToAdd)")
        spark.sql(s"ALTER TABLE $hrmnzdHiveTableName set tblproperties('external'='true')")
      }
    }

    dropFields
  }

  def alterDF(df: DataFrame, tableName: String, hiveSecuredDB: String, s3Path: String)
             (implicit spark: SparkSession): DataFrame = {

    val hiveTableName = if (hiveSecuredDB != "")
                                s"$hiveSecuredDB.${tableName.split("\\.")(1)}"
                                  else
                                  tableName
    val dropFields = alterSchema(df,hiveTableName, hiveSecuredDB)
    if(stgLoadBatch && dropFields.size > 0) {
      val newSchema= spark.read.table(hiveTableName).
        schema.fields.
        filter(col => !dropFields.contains(col.name)).
        map(field => field.name + " " + field.dataType.typeName).mkString(",")
      val s3PrimLocation = s3Path.replaceAll("/temp/","/")
      spark.sql(s"drop table $hiveTableName")
      spark.sql(
        s"""CREATE TABLE $hiveTableName($newSchema)
           |USING parquet
           |OPTIONS (path='$s3PrimLocation')
           |PARTITIONED BY (ingestiondt, bucket)
           |""".stripMargin)
      Holder.log.info("create table: " + s"""CREATE TABLE $hiveTableName($newSchema)
                                            |USING parquet
                                            |OPTIONS (path='$s3Path')
                                            |PARTITIONED BY (ingestiondt, bucket)
                                            |""".stripMargin)
      spark.sql(s"ALTER TABLE $hiveTableName RECOVER PARTITIONS")
      df
      /*dropFields.
          map(field => field.split("\\ ")).
          foldLeft(df)((acc, field) => acc.withColumn(field(0), lit(null).cast(field(1))))*/
    } else df
  }

  def apply(dftos3: Dataset[Row], hrmnzdHiveTableName: String,
            hiveSecureDB: String, s3path: String)(implicit spark: SparkSession) = {
    alterDF(dftos3, hrmnzdHiveTableName, hiveSecureDB, s3path)
  }
}
