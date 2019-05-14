package CSVtoHDFS;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class HDFS_Converter {

	private SparkSession orCreate;
	private Set<String> csv_files;
	private String path;

	public HDFS_Converter(String path) throws Exception {

		if(path.trim().isEmpty() || !path.contains("CSV")){
			throw new Exception("illegal path \n path should contain \"CSV\" folder");
		}

		this.csv_files = new HashSet<>();
		this.path = path;

	}

	public HDFS_Converter config(){

		SparkConf sparkConf = new SparkConf()
				  .setMaster("local")
				  .setAppName("SQLSPARK");

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkContext sparkContext = new SparkContext(sparkConf);

		sparkContext.setLogLevel("ERROR");

		this.orCreate = SparkSession.builder()
				  .appName("Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .config("spark.sql.orc.impl", "native")
				  .getOrCreate();

		return this;
	}

	public HDFS_Converter read() throws Exception {

		if(this.orCreate == null)
			throw new Exception("CSV folder hasn't been read yet");

		File file = new File(this.path);
		readFiles(file);

		return this;

	}

	public HDFS_Converter convert(HDFS_type type) throws Exception {

		if(csv_files.isEmpty())
			throw new Exception("CSV folder hasn't been read yet");

		if(type == HDFS_type.ORC)
			convertOrc();
		else if(type == HDFS_type.AVRO)
			convertAvro();
		else if(type == HDFS_type.PARQUET)
			convertParquet();

		return this;

	}

	private void convertOrc(){
		csv_files.forEach(file -> {

			String new_path = file
					  .substring(0, file.lastIndexOf("."))
					  .replace("\\CSV","\\ORC");

			System.out.println(file + " - " + new_path);

			convertToHDFS(file, new_path, "orc");

		});
	}

	private void convertAvro(){
		csv_files.forEach(file -> {

			String new_path = file
					  .substring(0, file.lastIndexOf("."))
					  .replace("\\CSV","\\Avro");

			System.out.println(file + " - " + new_path);

			convertToHDFS(file, new_path, "com.databricks.spark.avro");

		});
	}

	private void convertParquet(){
		csv_files.forEach(file -> {

			String new_path = file
					  .substring(0, file.lastIndexOf("."))
					  .replace("\\CSV","\\Parquet");

			System.out.println(file + " - " + new_path);

			convertToHDFS(file, new_path, "parquet");

		});
	}

	private void convertToHDFS(String path, String newPath, String format){

		try {
			FileUtils.deleteDirectory(new File(newPath));
		} catch (Exception e) {
			e.printStackTrace();
		}

		Dataset<Row> rowDataset = orCreate.read()
				  .format("csv")
				  .option("header", "true")
				  .option("inferSchema", "true")
				  .load(path) // csv path here
				  .toDF();

		rowDataset.write()
				  .format(format)
				  .save(newPath); // hdfs path here

	}

	private void readFiles(final File folder){

		for(final File fileEntry : Objects.requireNonNull(folder.listFiles()))
			if (fileEntry.isDirectory()) {
				readFiles(fileEntry);
			} else {
				this.csv_files.add(fileEntry.getAbsolutePath());
			}

	}

}
