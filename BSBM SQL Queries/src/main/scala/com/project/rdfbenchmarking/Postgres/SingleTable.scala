package com.project.rdfbenchmarking.Postgres

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

    sparkSession.time(
      sparkSession.sql(new SingleTableQueries query6).show()
    )

    sparkSession.time(
      sparkSession.sql(new SingleTableQueries query9).show()
    )

    sparkSession.time(
      sparkSession.sql(new SingleTableQueries query10).show()
    )

    sparkSession.time(
      sparkSession.sql(new SingleTableQueries query11).show()
    )

    sparkSession.time(
      sparkSession.sql(new SingleTableQueries query12).show()
    )

  }

}
