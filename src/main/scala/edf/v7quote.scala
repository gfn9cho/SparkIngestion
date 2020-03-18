package edf
import edf.utilities.{MetaInfo, TraversableOnceExt, sqlQueryParserFromCSV}

import scala.collection.immutable.Map

package object v7quote {

  implicit lazy val implicitConversions = scala.language.implicitConversions

  implicit def commomExtendTraversable[A, C[A] <: TraversableOnce[A]](coll: C[A]): TraversableOnceExt[C[A], A] =
    new TraversableOnceExt[C[A], A](coll, identity)

  implicit def commomExtendStringTraversable(string: String): TraversableOnceExt[String, Char] =
    new TraversableOnceExt[String, Char](string, implicitly)

  implicit def commomExtendArrayTraversable[A](array: Array[A]): TraversableOnceExt[Array[A], A] =
    new TraversableOnceExt[Array[A], A](array, implicitly)

  val propertyMap: Map[String, String] = sqlQueryParserFromCSV
    .getPropertyFile("diProperties.properties")
    .map(props => (props.propertyName, props.PropValue)).toMap

  val splitString = "@@##@@"
  val targetDB: String = propertyMap.getOrElse("spark.v7Quote.targetDB", "")
  val targetSecuredDB: String = propertyMap.getOrElse("spark.v7Quote.targetSecuredDB", "")
  val hrmnzds3Path: String = propertyMap.getOrElse("spark.v7Quote.hrmnzds3Path", "")
  val harmonizeds3SecurePath: String = propertyMap.getOrElse("spark.v7Quote.hrmnzds3SecurePath", "")
  val processName: String = propertyMap.getOrElse("spark.v7Quote.processName", "")
  val lookUpFile: String = propertyMap.getOrElse("spark.v7Quote.lookUpFile", "")
  val tableFile: String = propertyMap.getOrElse("spark.v7Quote.tableFile", "")
  val historyFileLocation: String = propertyMap.getOrElse("spark.v7Quote.historyFileLocation", "")

  val metaInfoForLookupFile: MetaInfo = new MetaInfo(lookUpFile,tableFile)
  val metaInfoForTableFile: MetaInfo = new MetaInfo(lookUpFile,tableFile)
  val refTableListFromLookUp: Set[String] = metaInfoForLookupFile.getTableList
  val refTableListFromTableSpec: Iterator[String] = metaInfoForTableFile.getTypeTables
  val mainTableListFromTableSpec: List[String] = metaInfoForTableFile.getTableSpec.map(_._1)
  val refTableList: List[String] = (refTableListFromLookUp ++ refTableListFromTableSpec).toList.distinct
  val ref_col_list = metaInfoForLookupFile.getRefColList
  val refTableListStr: String = refTableList.mkString(splitString)
  val piiFile: String = propertyMap.getOrElse("spark.v7Quote.piiFile", "")
  val piiListInfo: Map[String, List[(String, String)]] = new MetaInfo(piiFile,tableFile).getPiiInfoList
  val lookUpListInfo: Map[String, List[(String, String, String)]] = metaInfoForLookupFile.getLookUpData
  val tableInfos: Seq[(String, String, String, String)] = metaInfoForLookupFile.getLookUpInfo

  val tableSpec: List[(String,String)] = metaInfoForTableFile.getTableSpec

  val tableIngestionList: List[String] = tableSpec.map(_._1)
  val tableSpecMap: Map[String, String] = tableSpec.toMap
  val tableSpecMapTrimmed: Map[String, String] = tableSpecMap.map{case (k,v) => k.split("\\.")(2).toLowerCase -> v}
  val refTableListfromTableSpec: String = metaInfoForTableFile.getTypeTables.mkString(splitString)

  val getLookupCols = {
    lookUpListInfo.values.flatMap(list =>
      list.map(info => (info._1.split("\\.")(2), info._3))).groupBy(_._1).
      map{ case(k,v) => (k, v.map(_._2))}
  }

  val piiListFromLookup: List[(String, String)] = piiListInfo.toList.flatMap(pii =>
    lookUpListInfo.get(pii._1) match {
      case Some(lkp) => lkp.map(lkpValues => (lkpValues._1, lkpValues._3))
      case None => None
    })

  val piiListMultiMap: Map[String, List[String]] = (piiListFromLookup ++ piiListInfo.values.flatten.toList).toMultiMap

}
