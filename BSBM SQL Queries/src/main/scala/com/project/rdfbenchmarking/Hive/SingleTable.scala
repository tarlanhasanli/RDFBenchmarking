package com.project.rdfbenchmarking.Hive

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

    val warehouseLocation = "/user/hive/warehouse"

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://quickstart.cloudera:9083")
      .enableHiveSupport()
      .getOrCreate()

    val RDF_DF = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/SingleTable/SingleTable.csv")
      .toDF()

    RDF_DF.createOrReplaceTempView("SingleTableDF")
    sparkSession.sql("create table if not exists SingleTable as select * from SingleTableDF")

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
