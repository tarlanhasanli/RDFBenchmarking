package com.project.rdfbenchmarking.CSV

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

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .enableHiveSupport()
      .getOrCreate()

    val RDF_DF_OFFER = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Offer.csv").toDF()
    RDF_DF_OFFER.createOrReplaceTempView("Offer")

    val RDF_DF_Person = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Person.csv").toDF()
    RDF_DF_Person.createOrReplaceTempView("Person")

    val RDF_DF_Producer = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/producer.csv").toDF()
    RDF_DF_Producer.createOrReplaceTempView("Producer")

    val RDF_DF_Product = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/product.csv").toDF()
    RDF_DF_Product.createOrReplaceTempView("Product")

    val RDF_DF_ProductFeature = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/productFeature.csv").toDF()
    RDF_DF_ProductFeature.createOrReplaceTempView("ProductFeature")

    val RDF_DF_ProductType = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/ProductType.csv").toDF()
    RDF_DF_ProductType.createOrReplaceTempView("ProductType")

    val RDF_DF_Review = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/Review.csv").toDF()
    RDF_DF_Review.createOrReplaceTempView("Review")

    val RDF_DF_Vendor = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/PropertyTables/vendor.csv").toDF()
    RDF_DF_Vendor.createOrReplaceTempView("Vendor")



    /*
		 * Query 6: Find products having a label that contains a specific string.
		 *
		 * The consumer remembers parts of a product name from former searches.
		 * He wants to find the product again by searching for the parts of the name that he remembers.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

    /*
		 * Query 9: Get information about a reviewer.
		 *
		 * In order to decide whether to trust a review, the consumer asks for any
		 * kind of information that is available about the reviewer.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

    /*
		 * Query 10: Get offers for a given product which fulfill specific requirements.
		 *
		 * The consumer wants to buy from a vendor in the United States that is able to
		 * deliver within 3 days and is looking for the cheapest offer that fulfills these requirements.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

    /*
		 * Query 11: Get all information about an offer.
		 *
		 * After deciding on a specific offer, the consumer wants to get all information
		 * that is directly related to this offer.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

    /*
		 * Query 12: Export information about an offer into another schemata.
		 *
		 * After deciding on a specific offer, the consumer wants to save information
		 * about this offer on his local machine using a different RDF schema.
		 * */

    sparkSession.time(
      sparkSession.sql(
        """
          |
        """.stripMargin).show()
    )

  }

}
