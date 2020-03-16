package edf.missingdataload

import edf.dataingestion.{claimcenterDatabaseName, driver, jdbcSqlConnStr, now, piiList, splitString, tableInfos, tableSpecMapTrimmed}
import edf.utilities.{Holder, TraversableOnceExt}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit}
import scala.language.higherKinds


import scala.collection.immutable.Map

object BuildDataFrame {
  implicit lazy val implicitConversions = scala.language.implicitConversions

  implicit def commomExtendTraversable[A, C[A] <: TraversableOnce[A]](coll: C[A]): TraversableOnceExt[C[A], A] =
    new TraversableOnceExt[C[A], A](coll, identity)

  implicit def commomExtendStringTraversable(string: String): TraversableOnceExt[String, Char] =
    new TraversableOnceExt[String, Char](string, implicitly)

  implicit def commomExtendArrayTraversable[A](array: Array[A]): TraversableOnceExt[Array[A], A] =
    new TraversableOnceExt[Array[A], A](array, implicitly)

  val tableGroup: Map[String, List[String]] = tableInfos.map(info =>
    (info._1.split("\\.")(2), info._2.toString + splitString + info._4.toString)).toList.toMultiMap

  def draftSrcQueryWithIdSplit(queryString: String, missingIds: String) = {
    missingIds.split(",").sliding(100,100).map(slidingList =>
    s"$queryString (${slidingList.mkString(",")})").toList.mkString(" union all ")
  }

  def buildMissingDataSet(MissingSourceRecords: (String, (String, Any, String)))
                         (implicit spark: SparkSession): (String, DataFrame, String, Any, List[String], String) = {
    val table = MissingSourceRecords._1
    val missingDataSet = MissingSourceRecords._2
    val missingIds = missingDataSet._1
    val maxHrmnzdTimeStamp = missingDataSet._2
    val missingDataBatch = missingDataSet._3
    val cdcColFromTableSpecStr = tableSpecMapTrimmed
                              .getOrElse(table, "").split(splitString, -1)
    val decryptColList = cdcColFromTableSpecStr(4).split(",", -1)

    def decryptFunction(colName: String) = s"CASE WHEN $colName > '' THEN $claimcenterDatabaseName.dbo.AESDecrypt($colName,$claimcenterDatabaseName.dbo.fnGetKeyAESKey()) ELSE NULL END AS ${colName}_decrypted, "

    val decryptCol = decryptColList match {
      case Array("") => ""
      case other => other.map(decryptFunction(_)).reduce(_ + _)
    }
    val queryString = s"select cast(null as datetime) as deleteTime, $decryptCol * from $table where id in "
    val srcQueryWithSplit = draftSrcQueryWithIdSplit(queryString, missingIds)
    //val missingIds = "2165,2166"
    val sourceQuery = s"($srcQueryWithSplit ) MissingDataSet"
    Holder.log.info("Missing Source Query: " + sourceQuery)
    val missingDataSetFromSource = spark.sqlContext.read.format("jdbc")
      .options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> sourceQuery)).load()
    val piiColList = piiList.
      map(piiList => (piiList._1.split("\\.")(2), piiList._2)).
      toMultiMap.getOrElse(table, List.empty[String])

    val typeTableList: List[String] = tableGroup.get(table) match {
      case Some(tables: List[String]) => tables
      case None => Nil
    }

    val dfWithTypeLists: DataFrame = DataFrameUtility.makeJoins(missingDataSetFromSource, typeTableList.zipWithIndex)

    val partitionByCol = cdcColFromTableSpecStr(2)
    val cdcCol = cdcColFromTableSpecStr(0)

    val datePart = now.toString("YYYY-MM-dd")
    val batchPart = missingDataBatch
    val uniqueId = concat(col(datePart).cast("Long"), col(partitionByCol))
    val hardDeleteFlag = cdcColFromTableSpecStr(1)

    //TODO - Calculate ingestiondt, batch, uniqueId. - Done
    val finalMissingDataSet = if (hardDeleteFlag == "id" || hardDeleteFlag == "cdc") {
      Holder.log.info(s"hardDeleteFlag: $hardDeleteFlag")
      dfWithTypeLists.
        withColumn("deleted_flag", lit(0)).
        withColumn("ingestiondt", lit(datePart)).
        withColumn("uniqueid", concat(col("ingestiondt"), col(partitionByCol))).
        withColumn("batch", lit(batchPart))
    } else  {
      dfWithTypeLists.
        withColumn("ingestiondt", lit(datePart)).
        withColumn("batch", lit(batchPart)).
        withColumn("uniqueid", concat(col("batch"), col(partitionByCol)))
    }
    (table, finalMissingDataSet, missingIds, maxHrmnzdTimeStamp, piiColList, missingDataBatch)
  }

  def apply(missingData: (String, (String, Any, String)))(implicit spark: SparkSession) = {
    buildMissingDataSet(missingData)
  }
}
