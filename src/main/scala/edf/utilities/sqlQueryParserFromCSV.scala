package edf.utilities

import scala.io.Source

object sqlQueryParserFromCSV {

  case class lookUp(table: String, sourceCol: String, refCol: String,
                    reftable: String, harmnzdCol: String, comment: String)


  case class tableSpec(table: String, sendToLake: String, hasPII: String,
                       cdcCol: String, lookUpColRef: String, hardDeleteFlag: String,
                       partitionBy: String, numPartitions: String, decryptCol: String)



  case class piiSpec(table: String, piiCol: String)

  case class propertyFile(propertyName: String, PropValue: String)

  def lookUpData(fileName: String): Iterator[lookUp] = {
    for {
      line <- Source.fromFile(fileName).getLines().drop(1)
      values = line.split("\\|",-1).map(_.toLowerCase().trim)
    } yield lookUp(values(0), values(1), values(2), values(3), values(4), values(5))
  }

  def getTableSpecData(fileName: String): Iterator[tableSpec] = {
    for {
      line <- Source.fromFile(fileName).getLines().drop(1)
      values = line.split("\\|",-1).map(_.toLowerCase().trim)
      if(values(1)=="y")
    } yield {
      tableSpec(values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8))
    }
  }

  def getPiiSpecData(fileName: String): Iterator[piiSpec] = {
    for {
      line <- Source.fromFile(fileName).getLines().drop(1)
      values = line.split("\\|",-1).map(_.toLowerCase().trim)
    } yield piiSpec(values(0), values(1))
  }

  def getPropertyFile(propFile: String): Iterator[propertyFile] = {
    for {
      line <- Source.fromFile(propFile).getLines().filter(line => !(line.startsWith("#")  || line.startsWith("\t") || line.trim == ""))
      values = line.split(":",2)
    } yield propertyFile(values(0), values(1))
  }

  //val lookUpData = getLookUpData
  //println(lookUpData.size)
  //def getRefColList(fileName: String): List[String] = lookUpData(fileName).map(_.refCol).toList

  //def getRefTableSet(fileName: String): Set[String] = lookUpData(fileName).map(_.reftable.trim).toSet

  def getSqlStrings(fileName: String) = {

    val sqlStrings = lookUpData(fileName).map { lookUpTable => {
      if (lookUpTable.comment.contains("select")) {
        (lookUpTable.table, lookUpTable.reftable, lookUpTable.refCol, lookUpTable.comment)
      }
      else {
        val multiSelectHrmnzdCols = lookUpTable.harmnzdCol.split("/").map(_.trim)
        val multiSelectRefCols = lookUpTable.comment.split("/").map(_.trim)
        val aliasBuilder = multiSelectRefCols.zip(multiSelectHrmnzdCols).map {
          case (ref, hrmnzd) => "B." + ref + " as " + hrmnzd
        }.mkString(",")

        val sqlStr = "select " + aliasBuilder + " from " + lookUpTable.table + " A," +
          lookUpTable.reftable + " B where A." + lookUpTable.sourceCol + " = B." + lookUpTable.refCol
        (lookUpTable.table, lookUpTable.reftable, lookUpTable.refCol, sqlStr)
      }
    }
    }.toList
    sqlStrings
  }
}
