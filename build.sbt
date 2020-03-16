scalacOptions += "-feature"
val sparkVersion = "2.3.0"


lazy val root = (project in file(".")).
  settings(
    name := "datalake_processed",
    version := "2.1.0",

    scalaVersion := "2.11.7",
    mainClass in Compile := Some("edf.dataload.DataIngestion")
  )

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" ,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "joda-time" % "joda-time" % "2.10",
  "org.joda" % "joda-convert" % "2.0",
  "javax.mail" % "mail" % "1.4.7",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "org.apache.hudi" % "hudi-spark" % "0.5.0-incubating" % "provided"
)

unmanagedBase := baseDirectory.value / "lib"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("org", "objectweb", xs @ _*) => MergeStrategy.last
  case PathList("org", "eclipse", xs @ _*) => MergeStrategy.last
  case PathList("com", "amazonaws", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j-surefire.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "mime.types" => MergeStrategy.last
  case x if x.toLowerCase.contains("public-suffix-list.txt") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in (Test, assembly) := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll,
  ShadeRule.rename("org.eclipse.**" -> "shadeio.@1").inAll,
  ShadeRule.rename("org.apache.httpcomponents.**" -> "shadeio.@1").inAll
)
