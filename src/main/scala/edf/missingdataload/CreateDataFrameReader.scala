package edf.missingdataload

import org.apache.spark.sql.{DataFrameReader, SparkSession}

import scala.collection.immutable.Map
import edf.dataingestion._

object CreateDataFrameReader {
  def createDFReader(queryOptns: (String, String, Map[String, String]))
                    (implicit spark: SparkSession): (String, DataFrameReader) = {
    val table = queryOptns._1
    val query = queryOptns._2
    val optns = queryOptns._3
    //val numPartitions = optns.getOrElse("numPartitions", "1")
    table -> spark.sqlContext.read.format("jdbc")
      .options(Map("url" -> jdbcSqlConnStr, "Driver" -> driver, "dbTable" -> query) ++ optns)
  }

  def apply(options: (String, String, Map[String, String]))
           (implicit spark: SparkSession) = {
    createDFReader(options)
  }
}
