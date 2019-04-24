package com.project.rdfbenchmarking.Parquet

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

    val RDF_DF_OFFER = sparkSession.read.format("parquet").load("/user/cloudera/RDFBenchHDFS/Parquet/PropertyTables/Offer").toDF()
    RDF_DF_OFFER.createOrReplaceTempView("Offer")

    val RDF_DF_Person = sparkSession.read.format("parquet").load("/user/cloudera/RDFBenchHDFS/Parquet/PropertyTables/Person").toDF()
    RDF_DF_Person.createOrReplaceTempView("Person")

    val RDF_DF_Producer = sparkSession.read.format("parquet").load("/user/cloudera/RDFBenchHDFS/Parquet/PropertyTables/producer").toDF()
    RDF_DF_Producer.createOrReplaceTempView("Producer")

    val RDF_DF_Product = sparkSession.read.format("parquet").load("/user/cloudera/RDFBenchHDFS/Parquet/PropertyTables/product").toDF()
    RDF_DF_Product.createOrReplaceTempView("Product")

    val RDF_DF_ProductFeature = sparkSession.read.format("parquet").load("/user/cloudera/RDFBenchHDFS/Parquet/PropertyTables/productFeature").toDF()
    RDF_DF_ProductFeature.createOrReplaceTempView("ProductFeature")

    val RDF_DF_ProductType = sparkSession.read.format("parquet").load("/user/cloudera/RDFBenchHDFS/Parquet/PropertyTables/ProductType").toDF()
    RDF_DF_ProductType.createOrReplaceTempView("ProductType")

    val RDF_DF_Review = sparkSession.read.format("parquet").load("/user/cloudera/RDFBenchHDFS/Parquet/PropertyTables/Review").toDF()
    RDF_DF_Review.createOrReplaceTempView("Review")

    val RDF_DF_Vendor = sparkSession.read.format("parquet").load("/user/cloudera/RDFBenchHDFS/Parquet/PropertyTables/vendor").toDF()
    RDF_DF_Vendor.createOrReplaceTempView("Vendor")

    sparkSession.time(
      sparkSession.sql(new PropertyTableQueries query6).show()
    )

    sparkSession.time(
      sparkSession.sql(new PropertyTableQueries query9).show()
    )

    sparkSession.time(
      sparkSession.sql(new PropertyTableQueries query10).show()
    )

    sparkSession.time(
      sparkSession.sql(new PropertyTableQueries query11).show()
    )

    sparkSession.time(
      sparkSession.sql(new PropertyTableQueries query12).show()
    )

  }

}
