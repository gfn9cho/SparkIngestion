package edf.dataload

import edf.utilities.JobConfig
import org.apache.spark.sql.SparkSession

trait SparkJob {

  def main(args: Array[String]): Unit = {
    val appName = if(args.length == 0) "dataingestion" else args(args.length-1)

    implicit val spark = SparkSession.builder
      .appName(appName)
      .enableHiveSupport()
      //EDIN-363 : Restartability overwriting all data for DI
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      //EDIN-363 : Restartability overwriting all data for DI
      .getOrCreate()

    initiateAndRun(spark)

    def initiateAndRun(spark: SparkSession) = {
       JobConfig.getJobConfigProperties(spark) match {

        case propertyMap: Map[String, String] => run(spark, propertyMap)
        case _ => throw new Exception("Error Parsing the Job Config Properties File")
      }
    }
  }

  def run(spark: SparkSession, propertyConfigs: Map[String, String])


}
