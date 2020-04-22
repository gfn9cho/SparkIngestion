package edf.dataload.auditutilities

import java.nio.ByteBuffer

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import edf.dataload.{AuditSchema, auditDB, auditPath, ddbRegion, processName, schema, splitString}
import edf.utilities.Holder
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import scala.concurrent.ExecutionContext.Implicits.global


import scala.collection.immutable.Map
import scala.concurrent.Future

object AuditLog {
  def logAuditRow(stats: (String, Row), batchWindowStart: String, replTime: String)
                 (implicit spark: SparkSession) = {
    {

      val auditFrame = spark.createDataFrame(spark.sparkContext.parallelize(Seq(stats._2),1), schema)
      val auditSaveMode = if(spark.catalog.tableExists(s"$auditDB.audit"))
        SaveMode.Append else SaveMode.Overwrite
      auditFrame
        .filter(!col("batchwindowend").isNull)
        .withColumn("processname", lit(processName))
        .write.format("parquet")
        .partitionBy("processname", "ingestiondt")
        .options(Map("path" -> (auditPath + "/audit")))
        .mode(auditSaveMode).saveAsTable(s"$auditDB.audit")

      val auditData = stats._1 match {
        case "failed" => Some(stats._2.mkString(splitString))
        case "success" => None
      }

      (auditData, batchWindowStart, replTime, stats._2.getAs[String](12), stats._2.getAs[String](1), stats._2.getAs[String](14))
    }
  }
  import org.apache.spark.sql.functions.to_json
  def writeToStream(stats: (String, Row),batchWindowStart: String, replTime: String, region: Regions )
                   (implicit spark: SparkSession)= {
    import spark.implicits._
    val auditFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(stats._2),1), schema)
    Holder.log.info(s"auditStr: ${stats._2.mkString(",")}")
    val auditData = auditFrame.as[AuditSchema].first
    implicit val formats: DefaultFormats.type = DefaultFormats
    val data = write(auditData)
    //val data = stats._2.mkString(",")


   // Holder.log.info("writeToStream: " + rdd.toString() + ":" + stats._2.mkString(","))
    val credentials = getCredentials(ddbRegion, None)
    val streamName = "IngestionAuditLog"
    val endpointUrl = "https://kinesis.us-east-1.amazonaws.com"
    val kinesisClient = AmazonKinesisClientBuilder.standard()
      .withCredentials(credentials).withEndpointConfiguration(
      new AwsClientBuilder.EndpointConfiguration(endpointUrl, "us-east-1")).build()
      val request = new PutRecordRequest()
                            .withStreamName(streamName)
                            .withPartitionKey(s"$processName")
                            .withData(ByteBuffer.wrap(data.getBytes()))
   /* val request = PutRecordRequest(
      streamName   = "streamName",
      partitionKey = "partitionKey",
      data         = "data".getBytes("UTF-8")
    )*/
      kinesisClient.putRecord(request)
    // rdd.saveToKinesis("IngestionAuditLog", region, chunk = 1)
    (stats._2, batchWindowStart, replTime, stats._2.getAs[String](12),
      stats._2.getAs[String](1), stats._2.getAs[String](14))
  }
  private def getCredentials(chosenRegion: String, roleArn: Option[String]) = {
    roleArn.map(arn => {
      val stsClient = AWSSecurityTokenServiceClientBuilder
        .standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain)
        .withRegion(chosenRegion)
        .build()
      val assumeRoleResult = stsClient.assumeRole(
        new AssumeRoleRequest()
          .withRoleSessionName("DynamoDBAssumed")
          .withRoleArn(arn)
      )
      val stsCredentials = assumeRoleResult.getCredentials
      val assumeCreds = new BasicSessionCredentials(
        stsCredentials.getAccessKeyId,
        stsCredentials.getSecretAccessKey,
        stsCredentials.getSessionToken
      )
      new AWSStaticCredentialsProvider(assumeCreds)
    }).orElse(Option(System.getProperty("aws.profile")).map(new ProfileCredentialsProvider(_)))
      .getOrElse(new DefaultAWSCredentialsProviderChain)
  }

  def apply(stats: (String, Row), batchWindowStart: String, replTime: String)
           (implicit spark: SparkSession) = {
    Future {
      logAuditRow(stats,batchWindowStart,replTime)
    }
  }
  def apply(stats: (String, Row), batchWindowStart: String, replTime: String, region: Regions)
           (implicit spark: SparkSession) = {
    writeToStream(stats,batchWindowStart,replTime, region)
  }
}
