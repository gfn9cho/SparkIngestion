package edf.dataload.ddbutilities
import java.util

import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, KeySchemaElement}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, concat_ws, row_number}
import com.audienceproject.spark.dynamodb.implicits._

import scala.io.Source
import edf.dataload.{ddbRegion, ddbRoleArn, ddbTableMap, ddbThrougPut, loadType, propertyMap, writeToDynamoDB, writeUsingARN}
import edf.utilities.Holder
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest
import org.apache.spark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DecimalType, DoubleType, StringType, TimestampType}

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
      val sortKeyColumn = ddbTableInfo._4
      val idxList = ddbTableInfo._5.split(",").map(list => {
        val idxinfo  = list.split("\\|").map(_.trim)
        (idxinfo(0), idxinfo(1), idxinfo.lift(2))
      })
      val alteredSchema = df.schema.fields.map(c =>
        if(c.dataType.isInstanceOf[DecimalType]) c.copy(dataType=DoubleType) else
        if(c.dataType.isInstanceOf[TimestampType]) c.copy(dataType=StringType) else c)
        .map(x => col(x.name).cast(x.dataType))

      val dfWithKey = df.select(alteredSchema: _*).
                      withColumn(ddbKeyColumnName, concat_ws("-", srcKeyColList: _*))

    val dfPartitioned = if(loadType == "TL") {
      CreateDynamoTable.createTable(ddbTable, ddbKeyColumnName, sortKeyColumn, idxList)
        dfWithKey.
          repartition(400, col(ddbKeyColumnName)).
          write.option("throughput",ddbThrougPut)
      } else {
        val remDuplicates = dfWithKey.select(col(ddbKeyColumnName), col(sortKeyColumn)).
          withColumn("rnk", row_number().
            over(Window.partitionBy(col(ddbKeyColumnName)).
              orderBy(col(sortKeyColumn).desc))).where(col("rnk") === "1").drop("rnk")
        dfWithKey.join(broadcast(remDuplicates), Seq(ddbKeyColumnName, sortKeyColumn)).
          repartition(50, col(ddbKeyColumnName)).
          write.option("throughput", ddbThrougPut).option("update", true)
      }
    if(writeUsingARN) {
       dfPartitioned.
         option("region",ddbRegion).
         option("roleArn",ddbRoleArn).
         dynamodb(ddbTable)
    } else {
      dfPartitioned.
        dynamodb(ddbTable)
    }
     val count = Array("src", "tgt").par.map {
      case dest if dest == "src" => df.count
      case dest if dest == "tgt" => {
        val batch = df.select(col("batch")).dropDuplicates.first.getString(0)
        if(writeUsingARN)
        spark.read.option("region",ddbRegion).
          option("roleArn",ddbRoleArn).
          option("throughput",10000).
          dynamodb(ddbTable).where(col("batch") === batch).count
        else
          spark.read.
            option("throughput",10000).
            dynamodb(ddbTable).where(col("batch") === batch).count
      }
    }
    //val count = df.count()
    (count.head, count.last)
  }

  def apply(df: DataFrame, sourceTable: String)(implicit  spark: SparkSession) = {
    writeToDdbTable.writeToDynamoTable(df, sourceTable)
  }
}
