package com.project.rdfbenchmarking.Postgres

import com.project.rdfbenchmarking.Queries.PropertyTableQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PropertyTables {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.setLogLevel("ERROR")

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()


    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(sparkSession)

    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query1).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query2).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query3).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query4).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query5).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query6).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query7).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query8).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query9).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query10).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query11).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new PropertyTableQueries query12).show())

  }

}
