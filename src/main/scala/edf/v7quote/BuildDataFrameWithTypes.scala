package edf.v7quote

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map

object BuildDataFrameWithTypes {

  def getMainDFWithTLJoins(table: String, mainDF: DataFrame)
                          (implicit spark: SparkSession)= {
    val tableGroup: Map[String, List[String]] = tableInfos.map(info =>
      (info._1.split("\\.")(2), info._2.toString + splitString + info._4.toString)).
      toList.toMultiMap
    val typeTableList: List[String] = tableGroup.get(table) match {
      case Some(tables: List[String]) => tables
      case None => Nil
    }
    val dfWithTypeLists: DataFrame = DataFrameUtility.makeJoins(mainDF, typeTableList.zipWithIndex)
    dfWithTypeLists
  }

  def apply(table: String)(mainDF: DataFrame)(implicit spark: SparkSession) = {
    getMainDFWithTLJoins(table, mainDF)
  }
}
