package edf.v7quote

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.util.{Failure, Success, Try}
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
          val historyFileLocation: String = propertyMap.getOrElse("spark.v7Quote.historyFileLocation", "")
          val historyFile = s"$historyFileLocation/${table.toUpperCase}"
          val fileDF = spark.read.format("csv").
            option("header","false").
            option("delimiter","\t").
            option("inferSchema","false").
            option("mode","DROPMALFORMED").
            load(historyFile)
          def processedTable = CreateMainTableDF(table, fileDF).
              transform(BuildDataFrameWithTypes(table)).
                  transform(writeToS3(table))
          Try{
            fileDF.select(col("_c0")).
              except(processedTable.select(col("itemid")))
          } match {
            case Success(ids) => {
              Holder.log.info("Rejected Record IDs for table: " +
                table + ":" + ids.collect().mkString("--"))
            }
            case Failure(ex) => {
              Holder.log.info("Failed table: " +
                table + ":" + ex.getMessage)
            }
          }
        }
    }
}
