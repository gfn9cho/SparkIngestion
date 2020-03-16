package edf.recon

import edf.dataingestion.SparkJob
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

object ReconJob extends SparkJob{
  override def run(spark: SparkSession, propertyConfigs: Map[String, String]): Unit = {
    implicit val ss: SparkSession = spark
    gwRecon(gwclSourceQuery, gwclLakeQuery, "GWCL")
  }
}
