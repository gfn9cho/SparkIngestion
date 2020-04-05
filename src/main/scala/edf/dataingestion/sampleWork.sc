import edf.utilities.sqlQueryParserFromCSV.propertyFile

import scala.io.Source

val file = "D:\\Users\\CHO1190\\Documents\\serving.properties"


def getPropertyFile(propFile: String): Iterator[propertyFile] = {
  for {
    line <- Source.fromFile(propFile).getLines().
      filter(line => !(line.startsWith("#")  ||
        line.startsWith("\t") ||
        line.trim == ""))
    _ = println(line.split(":",2).mkString(","))
    _= println(line.split(":",2)(0))
    values = line.split(":")
  } yield propertyFile(values(0), values(1))
}

getPropertyFile(file).foreach(println(_))