package com.project.rdfbenchmarking.ORC

import com.project.rdfbenchmarking.Queries.SingleTableQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SingleTable {

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

    val RDF_DF = sparkSession.read
      .format("org.apache.spark.sql.execution.datasources.orc")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/user/cloudera/RDFBenchHDFS/ORC/SingleTable/SingleTable")
      .toDF()

    RDF_DF.createOrReplaceTempView("SingleTable")

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(sparkSession)

    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query1).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query2).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query3).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query4).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query5).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query6).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query7).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query8).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query9).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query10).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query11).show())
    stageMetrics.runAndMeasure(sparkSession.sql(new SingleTableQueries query12).show())

  }

}
