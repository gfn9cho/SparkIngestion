package edf.dataload.helperutilities

import org.apache.spark.sql.{SaveMode, SparkSession}
import edf.dataload.stageTablePrefix
import edf.utilities.Holder
import org.apache.spark.sql.functions.col

import scala.collection.immutable.Map

object BackUpHiveDB {
  def backup(tableList: List[(String, String)] ,hiveDB: String,hiveSecuredDB: String,
             backUpHiveDB: String, backupLocation: String)
            (implicit spark: SparkSession)= {
    tableList.par.foreach{table =>
      val stgTableName = s"$stageTablePrefix${table._1.split("\\.")(2)}"
      val sourceDF = if(spark.catalog.
        tableExists(s"$hiveSecuredDB.$stgTableName"))
        spark.sql(s"select * from $hiveSecuredDB.$stgTableName")
      else
        spark.sql(s"select * from $hiveDB.$stgTableName")

        sourceDF.
        write.format("parquet").
        partitionBy("ingestiondt","batch").
        options(Map("path" -> s"$backupLocation/$stgTableName")).
        mode(SaveMode.Overwrite).
        saveAsTable(s"$backUpHiveDB.$stgTableName")

      val count = Array("src", "tgt").par.map {
        case dest if dest == "src" =>
          sourceDF.count()
        case dest if dest == "tgt" =>
          spark.sql(s"select * from $backUpHiveDB.$stgTableName").count()
      }
      if(count.head == count.last) {
        if(spark.catalog.
          tableExists(s"$hiveSecuredDB.$stgTableName")) {
          spark.sql(s"drop table $hiveSecuredDB.$stgTableName")
        }
        spark.sql(s"drop table $hiveDB.$stgTableName")
      } else {
        Holder.log.info(s"back up failed for table:  $hiveDB.$stgTableName - Count Mismatch")
      }
    }
  }

  def apply(tableList: List[(String,String)], hiveDB: String, hiveSecuredDB: String, backupDB: String, backupLocation: String)
           (implicit spark: SparkSession)= {
    backup(tableList, hiveDB, hiveSecuredDB, backupDB, backupLocation)
  }
}
