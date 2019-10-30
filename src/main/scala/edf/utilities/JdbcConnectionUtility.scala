package edf.utilities

object JdbcConnectionUtility {

  def constructJDBCConnStr(propertyConfigs: Map[String, String]): String =
  {
    val dbHost = propertyConfigs.getOrElse("spark.DataIngestion.dbHost","")
    val dbUser = propertyConfigs.getOrElse("spark.DataIngestion.dbUser","")
    val dbPwd_enc = propertyConfigs.getOrElse("spark.DataIngestion.dbPwd","")    ///Password
    val dbPwd = Cipher(dbPwd_enc).simpleOffset(-5)
    val sourceDB = propertyConfigs.getOrElse("spark.DataIngestion.sourceDB","")
    val jdbcSqlConnStr = if(propertyConfigs.getOrElse("spark.DataIngestion.dbType","") == "Netezza")
      s"""jdbc:netezza://$dbHost:5480;database=$sourceDB;user=$dbUser;password=$dbPwd;"""
    else
      s"""jdbc:sqlserver://$dbHost:1433;database=$sourceDB;user=$dbUser;password=$dbPwd;"""
    jdbcSqlConnStr
  }

  //Gets Driver name for TL Tables
  def getTLJDBCDriverName(propertyConfigs: Map[String,String]): String = {
    val dbDriverMap = Map("SqlServer"->"com.microsoft.sqlserver.jdbc.SQLServerDriver","Netezza" -> "org.netezza.Driver")
    val dbTLType = propertyConfigs.getOrElse("spark.dataingestion.dbTLType",propertyConfigs.getOrElse("spark.DataIngestion.dbType", ""))
    dbDriverMap.getOrElse(dbTLType,"com.microsoft.sqlserver.jdbc.SQLServerDriver")
  }

  ///Constructs JDBC Connection for Typelist tables
  def constructTypeListJDBCConnStr(propertyConfigs: Map[String, String]): String = {
    val dbTLHost = propertyConfigs.getOrElse("spark.dataingestion.dbTLHost",
                                            propertyConfigs.getOrElse("spark.DataIngestion.dbHost", ""))
    val dbTLUser = propertyConfigs.getOrElse("spark.dataingestion.dbTLUser",
                                            propertyConfigs.getOrElse("spark.DataIngestion.dbUser", ""))
    val dbTLPwd_enc = propertyConfigs.getOrElse("spark.dataingestion.dbTLPwd",
                                            propertyConfigs.getOrElse("spark.DataIngestion.dbPwd", ""))    ///Password
    val dbTLPwd = Cipher(dbTLPwd_enc).simpleOffset(-5)
    val sourceTLDB = propertyConfigs.getOrElse("spark.dataingestion.sourceTLDB",
                                            propertyConfigs.getOrElse("spark.DataIngestion.sourceDB", "").replaceAll("\\[","").replaceAll("\\]","").replaceAll("-","_"))
    val jdbcTLSqlConnStr = s"""jdbc:sqlserver://$dbTLHost:1433;database=$sourceTLDB;user=$dbTLUser;password=$dbTLPwd;"""
    jdbcTLSqlConnStr
  }

  def getJDBCDriverName(propertyConfigs: Map[String,String]): String = {
    val dbDriverMap = Map("SqlServer"->"com.microsoft.sqlserver.jdbc.SQLServerDriver","Netezza" -> "org.netezza.Driver")
    val dbName = propertyConfigs.getOrElse("spark.DataIngestion.dbType","sqlServer")
    dbDriverMap.getOrElse(dbName,"com.microsoft.sqlserver.jdbc.SQLServerDriver")
  }
}
