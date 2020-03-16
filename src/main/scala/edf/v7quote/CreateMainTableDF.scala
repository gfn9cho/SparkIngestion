package edf.v7quote

import edf.dataload.{targetDB, propertyMap, getLookupCols}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_date, lit, col}

object CreateMainTableDF {

  def getData(tableName: String)(implicit  spark: SparkSession) = {
      val dfFromHive = spark.sql(s"select * from $targetDB.$tableName")
      val refCols = getLookupCols.getOrElse(tableName, List.empty[String]) ++
                            List("deletetime","ingestiondt", "batch")
      val hiveSchema = dfFromHive.drop(refCols.toList : _*)
    val historyFileLocation: String = propertyMap.getOrElse("spark.v7Quote.historyFileLocation", "")

    val historyFile = s"$historyFileLocation/${tableName.toUpperCase}"
      val fileDF = spark.read.format("csv").
      option("header","false").
      option("delimiter","\t").
      option("inferSchema","false").
        option("mode","DROPMALFORMED").
        load(historyFile)

      val fileSchema = fileDF.schema.fields.map(x => x.name)
      val tableSchema = hiveSchema.schema.fields.map(x => (x.name, x.dataType))
      val zipSchema = fileSchema.zip(tableSchema)
      val selectClause = zipSchema.map(x => col(x._1).cast(x._2._2).as(x._2._1))

      fileDF.select(selectClause: _*).
        withColumn("deletetime", lit(null).cast("Timestamp")).
        withColumn("ingestiondt", to_date(col("transactionyyyymm").cast("String"),"yyyyMM").cast("String")).
        withColumn("batch", lit("9999999999999"))
  }

  def apply(tableName: String)(implicit spark: SparkSession) = {
    getData(tableName)
  }
}
