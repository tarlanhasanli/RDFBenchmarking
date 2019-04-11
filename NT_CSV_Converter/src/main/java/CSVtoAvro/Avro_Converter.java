package CSVtoAvro;

import org.apache.log4j.*;
import org.apache.spark.*;
import org.apache.spark.sql.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Avro_Converter{

    SparkSession orCreate;

    public Avro_Converter(){

        File file = new File("/home/cloudera/RDFBenchmarking/Datasets/CSV");

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SQLSPARK");
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkContext sparkContext = new SparkContext(sparkConf);

        sparkContext.setLogLevel("ERROR");

        this.orCreate = SparkSession.builder()
                .appName("Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        listFilesForFolder(file);
    }


    private void convertToAvro(String path){

        String avro_path = path
                .substring(0, path.lastIndexOf("."))
                .replace("/CSV","/Avro");

        try {
            Files.deleteIfExists(Paths.get(avro_path));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Dataset<Row> rowDataset = orCreate.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(path) // csv path here
                .toDF();

        rowDataset.createOrReplaceTempView("RDF_Table");

        rowDataset.write()
                .format("com.databricks.spark.avro")
                .save(avro_path); // avro path here



    }

    private void listFilesForFolder(final File folder){

        for(final File fileEntry : folder.listFiles()){

            if(fileEntry.isDirectory()){

                listFilesForFolder(fileEntry);

            }else{

                convertToAvro(fileEntry.getAbsolutePath());

            }

        }

    }


}