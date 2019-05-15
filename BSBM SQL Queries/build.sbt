name := "BSBM SQL Queries"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"
libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.13"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1" % "provided"
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
