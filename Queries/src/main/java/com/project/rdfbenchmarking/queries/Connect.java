package com.project.rdfbenchmarking.queries;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class Connect {

    private SparkSession sparkSession;

    public Connect(){

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("SQL_Spark");

        Logger.getLogger("org")
                .setLevel(Level.OFF);
        Logger.getLogger("akka")
                .setLevel(Level.OFF);

        SparkContext sparkContext = new SparkContext(sparkConf);

        sparkContext.setLogLevel("ERROR");

        this.sparkSession = SparkSession.builder()
                .appName("Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();

    }

    public void createTempView(String format, String path, String tableName){

        DataFrameReader format1 = sparkSession.read().format(format);

        if(format.equalsIgnoreCase("csv"))
            format1 = format1.option("header", "true").option("inferSchema", "true");

        Dataset<Row> rowDataset = format1.load(path).toDF();

        rowDataset.createOrReplaceTempView(tableName);

    }

}
