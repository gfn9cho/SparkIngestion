package edf.dataingestion
import edf.utilities.{Holder, MetaInfo, sqlQueryParserFromCSV}

class CdcQueryBuilder(propertyConfigs: Map[String, String], tableSpecDetails: List[(String, String)]) {

  val coalescePattern = """coalesce\((.*),(.*)\)""".r
  val multiColPattern = """(.*,.*)+""".r

  val dbMap = Map("accounting" -> "acct", "policy" -> "plcy", "contact" -> "cntct", "claims" -> "clms")
  val loadType = propertyConfigs.getOrElse("spark.DataIngestion.loadType","")

  val tableSpecSet = tableSpecDetails.toSet

  def draftCdcQuery() = {
    tableSpecSet.map(list => {
      //Holder.log.info("Capturing tableSpec Info: " + list._1 + "," + list._2)
      val table_arr = list._1.split("\\.")
      val cdcColStr = list._2.split(splitString,-1)
      val cdcCol = cdcColStr(0)
      val partitionBy = cdcColStr(2)
      val table = table_arr(2)
      /*
      * EDIN-***: Start Instead of hard coding the suffix of hive database now we are passing the whole DB name from property file
      */
      //val hiveDB = propertyConfigs.getOrElse("spark.DataIngestion.targetDB","") + dbMap.getOrElse(table_arr(0),"") + "_data_processed"
      val hiveDB = propertyConfigs.getOrElse("spark.DataIngestion.targetDB","")
      /*
      * EDIN-***: End Instead of hard coding the suffix of hive database now we are passing the whole DB name from property file
      */
      Holder.log.info("hiveDB_Cdc: " + hiveDB)
      Holder.log.info("dbMaptable_arr: " + dbMap.getOrElse(table_arr(0),""))

      cdcCol match {
        case coalescePattern(col1: String, col2: String) => {
          if (loadType == "TL") {
            //Holder.log.info("Capturing draft cdc Query:" + s"""select min(${table}TrId) as lower, max(${table}TrId) as upper  from ${list._1}:${table}TrId""")
            (table, s"""select min($partitionBy) as lower, max($partitionBy) as upper  from ${list._1}:$partitionBy""")
          }
          else {
            (table, s"""select (case when max(${col1}) > max(${col2}) then max(${col1}) else max(${col2}) END) as cdc from ${hiveDB}.${table}:coalesce(${col1},${col2})""")
          }
        }
        case multiColPattern(cols: String) => {
          ///TODO - implement alias
          //TODO - implement truncate and load logic
          val maxCols = cols.split(",").map(col => "max(" + col + ") as " + col).mkString(",")
          (table, s"select ${maxCols} from ${hiveDB}.${table}:${maxCols}")
        }
        case clmn: String =>  {
          //TODO jdbc partition Column needs to be handled
          if (loadType == "TL")
            (table, s"select min($partitionBy) as lower ,max($partitionBy) as upper from ${list._1}:$partitionBy")
          else
            (table, s"select max(${clmn}) as cdc from ${hiveDB}.${table}:${clmn}")

        }
        case _ => (list._1, "")
      }
    }
    ).toMap
  }
}
