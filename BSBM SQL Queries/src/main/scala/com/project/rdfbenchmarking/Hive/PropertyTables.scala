package com.project.rdfbenchmarking.Hive

import com.project.rdfbenchmarking.Queries.PropertyTableQueries
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class PropertyTables {

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

    val RDF_DF_OFFER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Offer.csv").toDF()
    RDF_DF_OFFER.createOrReplaceTempView("OfferDF")
    sparkSession.sql("create table if not exists Offer as select * from OfferDF")

    val RDF_DF_Person = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Person.csv").toDF()
    RDF_DF_Person.createOrReplaceTempView("PersonDF")
    sparkSession.sql("create table if not exists Person as select * from PersonDF")

    val RDF_DF_Producer = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/producer.csv").toDF()
    RDF_DF_Producer.createOrReplaceTempView("ProducerDF")
    sparkSession.sql("create table if not exists Producer as select * from ProducerDF")

    val RDF_DF_Product = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/product.csv").toDF()
    RDF_DF_Product.createOrReplaceTempView("ProductDF")
    sparkSession.sql("create table if not exists Product as select * from ProductDF")

    val RDF_DF_ProductFeature = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/productFeature.csv").toDF()
    RDF_DF_ProductFeature.createOrReplaceTempView("ProductFeatureDF")
    sparkSession.sql("create table if not exists ProductFeature as select * from ProductFeatureDF")

    val RDF_DF_ProductType = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/ProductType.csv").toDF()
    RDF_DF_ProductType.createOrReplaceTempView("ProductTypeDF")
    sparkSession.sql("create table if not exists ProductType as select * from ProductTypeDF")

    val RDF_DF_Review = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Review.csv").toDF()
    RDF_DF_Review.createOrReplaceTempView("ReviewDF")
    sparkSession.sql("create table if not exists Review as select * from ReviewDF")

    val RDF_DF_Vendor = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/vendor.csv").toDF()
    RDF_DF_Vendor.createOrReplaceTempView("VendorDF")
    sparkSession.sql("create table if not exists Vendor as select * from VendorDF")

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
