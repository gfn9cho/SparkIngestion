package edf.dataload.dfactions

import edf.dataload._
import edf.dataload.dfutilities.BuildDeleteRecords
import edf.utilities.Holder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


object StageIncremental {
    def identifyPartitions(df: DataFrame, hardDeleteDF: DataFrame, stageTableName: String,
                           batch: String, isDeleteTable: Boolean, s3Loc: String)
                          (implicit spark: SparkSession) = {
      val incrDF = df.select(col("id"))
                      .withColumn("bucket", pmod(col("id"), lit(10)))
      val schema = StructType(
        StructField("id", LongType, true) ::
          StructField("bucket", LongType, true) :: Nil)
      val hardDeleteDFWithBucket = if (isDeleteTable)
                          //getHardDeletesFromHrmnzd(stageTableWithoutDB, batch)
                          hardDeleteDF.select(col("id"))
                           .withColumn("bucket", pmod(col("id"), lit(10)))
                          else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      val stageDF = spark.read.parquet(s3Loc).
                    withColumn("RecordType", lit("U")).
                    withColumn("destFile", input_file_name)

      val combinedDF = broadcast(incrDF.union(hardDeleteDFWithBucket)).
                        join(stageDF, Seq("id","bucket"), "left")
      val upsertDF = combinedDF.
                            where(col("RecordType") === "U").
                            select(col("destFile"), col("id"))

        if( !upsertDF.cache.head(1).isEmpty) {
          Holder.log.info("upsertDF: " + upsertDF.collect())
          val deleteFileList =   upsertDF.collect().
            map(x => (x.getString(0), x.getLong(1))).
            groupBy(_._1).map { case (k, v) => k -> v.map(_._2) }.toList.par.map(x => {
            val oldDF = spark.read.format("parquet").load(x._1).
              filter(!col("id").isin(x._2.toList: _*))
            val s3LocPattern = """(.*)(part.*parquet)""".r
            val s3LocPattern(s3TableLocation, _) = x._1
            oldDF.write.format("parquet").mode(SaveMode.Append).
              save(s"$s3TableLocation")
            x._1
          })
          deleteStagePartition(deleteFileList.toList)
        }
    }

  def duplicatesCheck(stageTableName: String, s3Location: String)
                     (implicit spark: SparkSession) = {
    val dupDF = spark.read.parquet(s3Location).
                    groupBy(col("id")).count.
                    where(col("count") > 1)

    if(!dupDF.head(1).isEmpty) {
      val stageDF = spark.read.parquet(s3Location).
        withColumn("fileName", input_file_name())
      val dupData = stageDF.
                            join(broadcast(dupDF.drop(col("count"))),
                                Seq("id"),"inner")
      val uniqueDF = dupData.groupBy(col("id")).
                              agg(max("batch").as("batch"))
      val dupFiles = dupData.
                            select(col("fileName")).dropDuplicates().
                            collect()
      dupData.drop(col("fileName")).
        join(uniqueDF, Seq("id","batch")).coalesce(1).
        dropDuplicates.
        write.
        partitionBy("ingestiondt", "bucket")
        .mode(SaveMode.Append)
        .parquet(s3Location)
      deleteStagePartition(dupFiles.map(_.getString(0)).toList)
    }

  }

  def getHardDeletesFromHrmnzd(tableName: String, batch: String="9999999999")
                              (implicit spark: SparkSession) = {
    val datePartition = now.toString("YYYY-MM-dd")
    val sourceDBName = tableSpecMap.head._1.split("\\.")
    val sourceTableName = s"${sourceDBName(0)}.${sourceDBName(1)}.${tableName.replaceAll(stageTablePrefix,"")}"
    val cdcColFromTableSpecStr = tableSpecMap.getOrElse(sourceTableName, "").split(splitString, -1)
    val deleteRecordsView = BuildDeleteRecords(sourceTableName, cdcColFromTableSpecStr, datePartition, batch)
    spark.table(deleteRecordsView)
    /*val hrmnzdDb = propertyMap.getOrElse ("spark.DataIngestion.targetDB", "")
    spark.sql(s"select id, batch from $hrmnzdDb.${tableName.replaceAll(stageTablePrefix,"")}").join(
    spark.table("hardDeleteBatch"), Seq("batch"),"inner")*/
  }

  def deleteStagePartition(fileList: List[String]): Unit = {
    import scala.sys.process._
    val filePattern = raw"s3://([^/]+)/(.*)".r
    val filePattern(bucket, _) = fileList.head
    val batchList = fileList.sliding(500,500).toList
    val batchKeyList = batchList.map(list => {
      list.foldLeft(List.empty[String])((acc, file) => {
        val filePattern(_, keyName) = file
        acc :+ (s"{Key=$keyName}")
      })
    }
    )

    for(keyList <- batchKeyList) {
      val keys = keyList.mkString(",")
      val s3DeleteCmd = s"aws s3api delete-objects --bucket $bucket --delete Objects=[$keys],Quiet=false"
     // Holder.log.info("s3DeleteCmd: " + s3DeleteCmd)
      val deleteResult = s3DeleteCmd.!!
      Holder.log.info("deleteResult: " + deleteResult)
    }
  }


  def apply(df: DataFrame, hardDeleteDF: DataFrame, stageTableName: String,
            batch: String, isDeleteTable: Boolean, s3Loc: String)
           (implicit spark: SparkSession) = {
    identifyPartitions(df,hardDeleteDF, stageTableName, batch, isDeleteTable, s3Loc)
  }
}
