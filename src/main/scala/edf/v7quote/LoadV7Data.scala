package edf.v7quote

import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}
import edf.dataload.tableSpecMapTrimmed
import edf.utilities.Holder
object LoadV7Data extends SparkJob {
    override def run(spark: SparkSession, propertyConfigs: Map[String, String]) = {
      implicit val ss: SparkSession = spark
      spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
      spark.sparkContext.hadoopConfiguration.set("speculation", "false")
      //spark.conf.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
      spark.conf.set("spark.sql.parquet.mergeSchema","false")
      spark.conf.set("spark.sql.parquet.filterPushdown", "true")
      spark.conf.set("spark.sql.hive.metastorePartitionPruning","true")
      spark.conf.set("spark.hadoop.parquet.enable.summary-metadata","false")
        /*
        1. read table spec.
        2. For each table, read the history files
        3. For each table, build schema from hive table.
        4. if its a lookup table, manually build the dataframe sql.
        5. if its a pii table, load into secured bucket.
        6. if its a pii table, mask the respective column and load into harmonized.
       */
        tableSpecMapTrimmed.keys.foreach { table =>
          def processedTable = CreateMainTableDF(table).
              transform(BuildDataFrameWithTypes(table)).
                  transform(writeToS3(table))
          def count = Try{
            processedTable.count
          } match {
            case Success(count) => count
            case Failure(ex) => ex.getMessage
          }
          Holder.log.info("Processed Table: " + table + ": "+ count.toString
            )
        }
    }
}
