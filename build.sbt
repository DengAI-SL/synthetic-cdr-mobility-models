name := "synthetic-cdr-mobility-models"

version := "1.4.2"

scalaVersion := "2.12.15"

scalacOptions ++= Seq("-unchecked","-deprecation")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.2.1" % "provided",
  "org.apache.spark" % "spark-sql_2.12" % "3.2.1" % "provided"
)

lazy val commonSettings = Seq(
  organization := "lk.uom.datasearch",
  test in assembly := {}
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("lk.uom.datasearch.homework.AnnualHomeWorkClassifyModel"),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  )

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case "application.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
