package CSVtoParquet;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class Parquet_Converter {

    SparkSession orCreate;

    public Parquet_Converter(){

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


    private void convertToParquet(String path){

        String parquet_path = path
                .substring(0, path.lastIndexOf("."))
                .replace("/CSV","/Parquet");

        try {
            FileUtils.deleteDirectory(new File(parquet_path));
        } catch (Exception e) {
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
                .format("parquet")
                .save(parquet_path); // parquet path here

    }

    private void listFilesForFolder(final File folder){

        for(final File fileEntry : folder.listFiles()){

            if(fileEntry.isDirectory()){

                listFilesForFolder(fileEntry);

            }else{

                convertToParquet(fileEntry.getAbsolutePath());

            }

        }

    }


}