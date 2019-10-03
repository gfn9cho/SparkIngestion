package edf.utilities
import edf.utilities.sqlQueryParserFromCSV.lookUpData
class MetaInfo(lookupOrPiifile: String,fileName: String) {

    val splitString = "@@##@@"
    def getTableSpecDetails = sqlQueryParserFromCSV.getTableSpecData(fileName)
    def getTableSpec = getTableSpecDetails.filter(x =>
        x.lookUpColRef=="y" || x.lookUpColRef.trim=="" || x.lookUpColRef == "n").map(tables =>
        tables.table -> (tables.cdcCol + splitString + tables.hardDeleteFlag

          + splitString + tables.partitionBy + splitString + tables.numPartitions +
          splitString + tables.decryptCol)).toList

    def getTypeTables = getTableSpecDetails.filter(_.lookUpColRef.trim=="typelist").map(_.table)
    val tableList =  getTableSpec.map(_._1)
    def getLookUpInfo = sqlQueryParserFromCSV.getSqlStrings(lookupOrPiifile)
                                                      .filter(table => tableList.contains(table._1))
    def getRefColList =  lookUpData(lookupOrPiifile)
                                        .filter(tables => tableList.contains(tables.table))
                                        .map(_.refCol).toList
    def getPiiInfoList: Map[String, List[(String, String)]] = {
        sqlQueryParserFromCSV.getPiiSpecData(lookupOrPiifile).map(pii =>
            (pii.table.toLowerCase , pii.piiCol)).toList.groupBy(_._1)
    }

    def getTableList = lookUpData(lookupOrPiifile)
                                    .filter(tables => tableList.contains(tables.table))
                                    .map(_.reftable.trim).toSet

    def getLookUpData =
                                  sqlQueryParserFromCSV.lookUpData(lookupOrPiifile)
                                    .filter(table => tableList.contains(table.table)).map{
                                lookup => (lookup.table, lookup.reftable,
                                  lookup.harmnzdCol)
    }.toList.groupBy(_._2)

}
