package edf.utilities

object GenerateHtmlContent {

  val splitString = "@@##@@"
    def generateTable(data: List[String]) = {
      <table>
        {data.zipWithIndex.map { case (row, rownum) => if (rownum == 0) {
        <tr>
          {row.split(splitString).map(cell =>
            <th>
              {cell}
            </th>)}
        </tr>
      } else
      {
        <tr>
          {row.split(splitString).map(cell =>
          <td>
            {cell}
          </td>)}
        </tr>
      }
      }
      }
      </table>
    }.toString

  def generateContent(data: List[String], processName: String, batchStartTime: String, environment: String) = {
    val dataContent = data match {
      case _ :: data :: Nil =>
        val batchDetails = data.split(splitString)
        (batchDetails(0), batchDetails(1), batchDetails(11) )
      case _ :: data :: _ =>
        val batchDetails = data.split(splitString)
        (batchDetails(0), batchDetails(1), batchDetails(11) )
      case _ => ("","","")
    }

    val contentStr = s"""<html>
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
<body aria-readonly="false">There are failed data loads in today's run.
Refer the process details below,
<br />
<b>Environment:       </b>${environment}<br/>
<b>Process Name:      </b>${processName}<br />
<b>Load Type:         </b>${dataContent._3}<br />
<b>Batch Start Time:  </b>${batchStartTime}<br />
<b>Batch Partition:   </b>${dataContent._2}<br />
<b>Date Partition:    </b>${dataContent._1}<br />
<br />

Below are the details of the list of tables failed in the last run,<br />
<br /> """ +
generateTable(data) + s"""
</body>
</html>"""

    contentStr
  }

}
