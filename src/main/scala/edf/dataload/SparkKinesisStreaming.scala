package edf.dataload

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.{Region, RegionUtils}
import com.amazonaws.services.kinesis.{AmazonKinesisClient, AmazonKinesisClientBuilder}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import edf.utilities.Holder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.TrimHorizon
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream, KinesisUtils}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}

import scala.collection.immutable.Map

object SparkKinesisStreaming extends SparkJob {

  override def run(spark: SparkSession, propertyConfigs: Map[String, String]): Unit = {
    implicit val ss: SparkSession = spark
    spark.conf.set("spark.driver.allowMultipleContexts",true)
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    spark.sparkContext.hadoopConfiguration.set("speculation", "false")

    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.hive.metastorePartitionPruning", "true")
    spark.conf.set("spark.hadoop.parquet.enable.summary-metadata", "false")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", partitionOverwriteMode)
    spark.conf.set("spark.sql.broadcastTimeout", "30000")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    //val conf = new SparkConf().setAppName("Kinesis Read Sensor Data")
    //conf.setIfMissing("spark.master", "local[*]")
    //conf.set("spark.driver.allowMultipleContexts","true")

    val appName = "sa360Streaming"
    val streamName = "IngestionAuditLog"
    val endpointUrl = "https://kinesis.us-east-1.amazonaws.com"

    val regionName = "us-east-1"
    val credentials = getCredentials(ddbRegion, None)
    val kinesisClient = AmazonKinesisClientBuilder.standard()
        .withCredentials(credentials).withEndpointConfiguration(
      new AwsClientBuilder.EndpointConfiguration(endpointUrl, "us-east-1")).build()

    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size

    val numStreams = numShards
    Holder.log.info("numStream: " + numStreams)
    val batchInterval = Milliseconds(60000)

    val kinesisCheckpointInterval = batchInterval

    // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
    // DynamoDB of the same region as the Kinesis stream

    val ssc = new StreamingContext(spark.sparkContext, batchInterval)

    // Create the Kinesis DStreams
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .endpointUrl(endpointUrl)
      .regionName(regionName)
      .streamName(streamName)
      .initialPosition(new TrimHorizon())
      .checkpointAppName(appName)
      .checkpointInterval(kinesisCheckpointInterval)
      .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }

    // Union all the streams (in case numStreams > 1)
    val unionStreams = ssc.union(kinesisStreams)
    import spark.implicits._
    unionStreams.foreachRDD ((rdd: RDD[Array[Byte]], time: Time) => {
      val rowRDD = rdd.map(jstr => new String(jstr))
      val df = spark.sqlContext.read.schema(schema).json(rowRDD.toDS)
      df
        .withColumn("processname", lit(processName))
        .write.format("parquet")
        .partitionBy("processname", "ingestiondt")
        .options(Map("path" -> (auditPath + "/audit")))
        .mode(SaveMode.Append).saveAsTable(s"$auditDB.audit")

    })

    ssc.start()
    ssc.awaitTermination()
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


}
