package com.project.rdfbenchmarking.CSV

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
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val RDF_DF_OFFER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Offer.csv").toDF()
    RDF_DF_OFFER.createOrReplaceTempView("Offer")

    val RDF_DF_Person = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Person.csv").toDF()
    RDF_DF_Person.createOrReplaceTempView("Person")

    val RDF_DF_Producer = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/producer.csv").toDF()
    RDF_DF_Producer.createOrReplaceTempView("Producer")

    val RDF_DF_Product = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/product.csv").toDF()
    RDF_DF_Product.createOrReplaceTempView("Product")

    val RDF_DF_ProductFeature = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/productFeature.csv").toDF()
    RDF_DF_ProductFeature.createOrReplaceTempView("ProductFeature")

    val RDF_DF_ProductType = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/ProductType.csv").toDF()
    RDF_DF_ProductType.createOrReplaceTempView("ProductType")

    val RDF_DF_Review = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Review.csv").toDF()
    RDF_DF_Review.createOrReplaceTempView("Review")

    val RDF_DF_Vendor = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/vendor.csv").toDF()
    RDF_DF_Vendor.createOrReplaceTempView("Vendor")

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
