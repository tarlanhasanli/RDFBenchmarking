package CSVtoAvro;

import org.apache.log4j.*;
import org.apache.spark.*;
import org.apache.spark.sql.*;

public class Avro_Converter{

    public Avro_Converter(){

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SQLSPARK");
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkContext sparkContext = new SparkContext(sparkConf);

        sparkContext.setLogLevel("ERROR");

        SparkSession orCreate = SparkSession.builder()
                .appName("")
                .config("", "")
                .getOrCreate();

        Dataset<Row> rowDataset = orCreate.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("") // csv path here
                .toDF();

        rowDataset.createOrReplaceTempView("RDF_Table");

        rowDataset.write()
                .format("com.databricks.spark.avro")
                .save(""); // avro path here
    }


}