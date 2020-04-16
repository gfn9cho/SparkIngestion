package edf.dataload.ddbutilities
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws}
import com.audienceproject.spark.dynamodb.implicits._

import scala.io.Source
import edf.dataload.{ddbRegion, ddbRoleArn, ddbTableMap, ddbThrougPut, loadType, propertyMap, writeToDynamoDB}
import edf.utilities.Holder
import org.apache.spark
import org.apache.spark.sql.types.{DecimalType, DoubleType}

object writeToDdbTable {

  def writeToDynamoTable(df: DataFrame, sourceTable: String)
                        (implicit spark: SparkSession) = {
    Holder.log.info(ddbTableMap.mkString(",") + ":" + sourceTable)
      val ddbTableInfo = ddbTableMap.getOrElse(sourceTable,
        throw new Exception("dynamo table not found in the ddbTableMap"))
      val ddbTable = ddbTableInfo._1
      val ddbKeyColumnName = ddbTableInfo._2
      val srcKeyColList = ddbTableInfo._3.
                          split(",",-1).map(c => col(c.trim))
      val alteredSchema = df.schema.fields.map(c =>
        if(c.dataType.isInstanceOf[DecimalType]) c.copy(dataType=DoubleType) else c)
        .map(x => col(x.name).cast(x.dataType))

      val dfWithKey = df.select(alteredSchema: _*).
                      withColumn(ddbKeyColumnName, concat_ws("-", srcKeyColList: _*))
      val dfPartitioned = if(loadType == "TL")
                          dfWithKey.
                            repartition(400, col(ddbKeyColumnName)).
                            write.option("throughput",4000)
                        else dfWithKey.
                              repartition(50, col(ddbKeyColumnName)).
                              write.option("throughput",4000).option("update",true)
       dfPartitioned.
         //option("region",ddbRegion).
         //option("roleArn",ddbRoleArn).
         dynamodb(ddbTable)
   /* val count = Array("src", "tgt").par.map {
      case dest if dest == "src" => df.count
      case dest if dest == "tgt" => {
        val batch = df.select(col("batch")).dropDuplicates.first.getString(0)
        spark.read.dynamodb(ddbTable).where(col("batch") === batch).count
      }
    }*/
    val count = df.count()
    (count, count)
  }

  def apply(df: DataFrame, sourceTable: String)(implicit  spark: SparkSession) = {
    writeToDdbTable.writeToDynamoTable(df, sourceTable)
  }
}
