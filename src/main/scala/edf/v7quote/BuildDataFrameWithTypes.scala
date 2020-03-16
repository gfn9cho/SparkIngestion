package edf.v7quote

import edf.dataload.{tableGroup}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map

object BuildDataFrameWithTypes {

  def getMainDFWithTLJoins(table: String, mainDF: DataFrame)
                          (implicit spark: SparkSession)= {

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
