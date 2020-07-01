name := "fifa"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.postgresql" % "postgresql" % "9.3-1102-jdbc41"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.2"