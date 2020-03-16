package edf.dataload.dfutilities

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col}

object JoinDataFrames {
  def joinDFs(dfL: DataFrame, dfR: DataFrame, conditions: (String, List[String]), joinType: String, selfJoinInd: Char) = {

    val (sCondition_full, jCondition_full) = (conditions._1.split(","), conditions._2)
    val colAliasPrefix = if (selfJoinInd == 'Y') "dfr_" else ""
    val sCondition = sCondition_full.map(_.replace(".", "").split("-") match {
      case Array(x: String, y: String) => {
        //Holder.log.info("####Capturing select fields inside parseCondition: " + x + ":" + y)
        col(`x`).as(y)
      }
      case Array(x: String) => {
        if (selfJoinInd == 'Y') col(`x`).as(colAliasPrefix + x) else col(x)
      }
    })

    val jCondition = jCondition_full.map(cond => {
      val condType = cond.split("-")
      condType(0) match {
        case "jSeq" => col(condType(1)) === col(colAliasPrefix + condType(1))
        case "jEqui" => {
          val condSplit = condType(1).split(":")
          col(condSplit(0)) === col(colAliasPrefix + condSplit(1))
        } case "wEqui" => {
          val condSplit = condType(1).split(":")
          col(colAliasPrefix + condSplit(0)) === condSplit(1)
        }
        case "wNull" => col(colAliasPrefix + condType(1)).isNull
      }
    }
    ).reduce(_ and _)

    val dPattern = """jEqui-.*|wEqui-.*|wNull-.*""".r
    val dCondition: List[String] = jCondition_full.map(cond => {
      dPattern.findFirstIn(cond) match {
        case Some(clmn: String) => {
          val replacePattern = """jEqui-.*:|wEqui-|wNull-""".r
          val dropclmn = replacePattern.replaceFirstIn(clmn, "")
          //if(selfJoinInd == 'Y') "dfr_" + dropclmn else dropclmn
          colAliasPrefix + dropclmn.split(":")(0)
        }
        case None => ""
      }
    }
    )
    dfL.join(broadcast(dfR.select(sCondition: _*).dropDuplicates), jCondition, joinType).drop(dCondition: _*)
  }
}
