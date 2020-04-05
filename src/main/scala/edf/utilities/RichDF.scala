package edf.utilities

import edf.dataload.processName
import org.apache.spark.sql.DataFrame

object RichDF {
   implicit def showHTML(ds:DataFrame, limit:Int = 20, truncate: Int = 20) = {
    import xml.Utility.escape
    val data = ds.take(limit)
    val header = ds.schema.fieldNames.toSeq
    val rows: Seq[Seq[String]] = data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }

    val htmlStr = s"""
                      <html>
<head>
        <meta charset="utf-8">
        <style>
            table {
                border-collapse: collapse;
                border: 2px black solid;
                font: 12px sans-serif;
            }

            td {
                border: 1px black solid;
                padding: 5px;
            }
            th{
                border: solid 1px;
                padding: 5px;
                background-color: #ccc;
            }
        </style>
</head>
<body aria-readonly="false">
                   Staing Recon for $processName is complete. Below are the results,
                   <table>
                <tr>
                 ${header.map(h => s"<th>${escape(h)}</th>").mkString}
                </tr>
                ${rows.map { row =>
      s"<tr>${row.map{c => s"<td>${escape(c)}</td>" }.mkString}</tr>"
    }.mkString}
            </table>
            </body>
</html>
        """
  htmlStr
  }

   implicit def apply(ds:DataFrame, limit:Int = 20, truncate: Int = 20) = {
     showHTML(ds, limit, truncate)
   }
}