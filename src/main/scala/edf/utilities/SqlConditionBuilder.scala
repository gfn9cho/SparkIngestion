package edf.utilities

import scala.collection.mutable.ArrayBuffer

object SqlConditionBuilder {
  def parseCondition(idx: Int, sqlStr: String, ref_col_list: List[String]): (String, List[String]) = {

    val pattern = raw"select(.*)from(.*)where(.*).*".r.unanchored
    val replace = raw"\w*\.".r.unanchored
    val fieldPattern = raw"(\w*)\s+as\s+(\w*)".r.unanchored
    val condPattern = raw"(\w*)\s+=\s(\w*)".r.unanchored
    val isNullPattern = raw"(\w*)\sis\snull".r.unanchored

    //def getRefTableSet: Set[String] = lookUpData.map(_.reftable).toSet
    //val ref_col_list = new MetaInfo(lookUpFile).getRefColList
//Holder.log.info("sqlStr: " + sqlStr)
    val conditions: (String, List[String]) = sqlStr match {
      case pattern(x, y, z) => {
        val fields: Array[String] = x.split(",").map(replace.replaceAllIn(_, ""))
        //val tables: Array[String] = y.split(",").map(replace.replaceAllIn(_,""))
        val conditions: Array[String] = z.split(" and").map(replace.replaceAllIn(_, ""))

        val sparkField: Array[String] = fields.map(field =>
          field match {
            case fieldPattern(name: String, alias: String) => name + "-" + alias
            case _ => ""
          }
        )

        val sparkFieldMut = ArrayBuffer(sparkField: _*)

        val jCondition: List[String] = conditions.toList.map(condition => {
          condition match {
            case condPattern(left, right) if left == right  => {
              sparkFieldMut += left.mkString + "-" + left.mkString + "_" + idx
              "jEqui-" + left.mkString + ":" + left.mkString + "_" + idx
            }
            case condPattern(left, right) if right == "id"  => {
              sparkFieldMut += right.mkString + "-" + right.mkString + "_" + idx
              "jEqui-" + left.mkString + ":" + right.mkString + "_" + idx
            }
            case condPattern(left, right) if ref_col_list.contains(right)  => {
              sparkFieldMut += right.mkString
              "jEqui-" + left + ":" + right
            }
            case condPattern(left, right) => {
              Holder.log.info("inside CondPattern: " + "wEqui-" + left + "_" + idx + ":" + right)
              sparkFieldMut += left.mkString + "-" + left.mkString + "_" + idx
              "wEqui-" + left + "_" + idx + ":" + right
            }
            case isNullPattern(left) => {
              Holder.log.info("#####Capturing Null Column in ParseCondition" + left.mkString)
              sparkFieldMut += left.mkString + "-" + left.mkString + "_" + idx
              "wNull-" + left + "_" + idx
            }
          }
        })
        val sCondition: String = sparkFieldMut.mkString(",")
        //Holder.log.info("####Capturing sCondition inside parseCondition: " + sCondition)
        //val query = tableName + ".join(" + ref_table + ".where(" + wCondition + ").select(" + sparkField.mkString(",") + ")," + jCondition + ",\"left\")"
        (sCondition, jCondition)
      }
      case _ => ("", List(""))
    }
    conditions
  }
}