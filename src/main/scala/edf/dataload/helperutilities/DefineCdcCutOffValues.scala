package edf.dataload.helperutilities

import java.sql.Timestamp

import edf.dataload.{stgLoadBatch, timeLagInMins, timeZone, stageCutOffLimit, stageCutOffTime}
import edf.utilities.Holder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.joda.time.Minutes
import org.joda.time.format.DateTimeFormat

object DefineCdcCutOffValues {
  def getMinMaxCdc(tableDF: String, tableKey: String,
                   replicationTime: String)(implicit spark: SparkSession): (Column, Any, Any) = {
    val cdcColMaxStr = CdcColumnList.getCdcColMax(tableKey)
    if (cdcColMaxStr._2.endsWith("id") ||
      cdcColMaxStr._2.endsWith("yyyymm") ||
      cdcColMaxStr._2.endsWith("loadtime")) {
      val dateStrDF = spark.table(tableDF)
        .select(cdcColMaxStr._1.as("max_window_end"))
        .agg(min(col("max_window_end")), max(col("max_window_end")))
      val dateStr = dateStrDF.first()
      (cdcColMaxStr._1, dateStr.getAs[Long](0), dateStr.getAs[Long](1))
    } else {
      val coalesceDateHaving9999 = cdcColMaxStr._4
      val dateStrDF =
        if(coalesceDateHaving9999 == null)
          spark.table(tableDF)
            .select(cdcColMaxStr._1.as("max_window_end"))
            .agg(min(col("max_window_end")), max(col("max_window_end")))
        else
          spark.table(tableDF)
            .withColumn(coalesceDateHaving9999,
              when(col(coalesceDateHaving9999).startsWith("9999"),lit(null))
                .otherwise(col(coalesceDateHaving9999)))
            .select(cdcColMaxStr._1.as("max_window_end"))
            .agg(min(col("max_window_end")), max(col("max_window_end")))

      val dateStr = dateStrDF.first()
      Holder.log.info("dateStr: " + dateStr.mkString(",") + ":" + cdcColMaxStr.toString())
      val minDate = dateStr.getAs[Timestamp](0)
      val datetime_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.SSS")
      val currentReplTime = datetime_format.withZone(timeZone).parseDateTime(replicationTime)
      val currentReplTimeTruncated = if(stageCutOffLimit == "MIDNIGHT")
                                          currentReplTime.withTimeAtStartOfDay().minusMillis(1)
                                     else if(stageCutOffTime != "")
                                          datetime_format.withZone(timeZone).parseDateTime(stageCutOffTime)
                                     else currentReplTime.toDateTime()
      val maxDate = dateStr.getAs[Timestamp](1) match {
        case date: Timestamp =>
          val maxDate = datetime_format.withZone(timeZone).parseDateTime(date.toString)
          Holder.log.info("maxDate: " + maxDate)
          val timeLagInUpdates =   Minutes.minutesBetween(currentReplTime, maxDate).getMinutes

          val minutesBetweenMinAndMaxDates = Minutes.minutesBetween(maxDate,
            datetime_format.withZone(timeZone).parseDateTime(minDate.toString)).getMinutes
          val maxDateMinus2Mins = if(timeLagInUpdates > 30 ||
            minutesBetweenMinAndMaxDates == 0) maxDate else maxDate.minusMinutes(timeLagInMins)
          val cutOffMaxDate = stgLoadBatch match {
            case true => if (maxDateMinus2Mins.getMillis > currentReplTimeTruncated.getMillis)
              currentReplTimeTruncated else  maxDateMinus2Mins
            case false => maxDateMinus2Mins
          }
          Some(cutOffMaxDate.toString("YYYY-MM-dd HH:mm:ss.SSS"))
        //Some("2019-08-01 16:08:58.546")
        case _ => None
      }

      (cdcColMaxStr._1,minDate, maxDate.orNull)
    }
  }
}
