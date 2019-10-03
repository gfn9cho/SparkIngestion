package edf.utilities
import org.apache.spark.sql.SparkSession

object JobConfig {
  implicit lazy val implicitConversions = scala.language.implicitConversions
  def getJobConfigProperties(spark: SparkSession): Map[String, String] = {

    val propFile: String = spark.conf.getAll.get("spark.yarn.dist.files") match {
      case Some(file) => file.replace("file:", "")
      case _ => ""
    }
    val propertyMap = sqlQueryParserFromCSV.getPropertyFile("diProperties.properties").map(props => (props.propertyName, props.PropValue)).toMap
    propertyMap
  }

}
