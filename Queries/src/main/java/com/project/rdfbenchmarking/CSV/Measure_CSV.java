package com.project.rdfbenchmarking.CSV;

import com.project.rdfbenchmarking.Connect;
import com.project.rdfbenchmarking.queries.SingleTableQueries;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Measure_CSV {

	private final String format = "csv";
	private final String path = "hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/";
	private Connect connect;
	private SparkSession sparkSession;

	public Measure_CSV(){

		this.connect = new Connect();
		this.sparkSession = connect.getSparkSession();

	}

	public void measureSingleTable(){
		Dataset<Row> rowDataset = connect.createTempView(format, path+"SingleTable/SingleTable.csv");
		rowDataset.createOrReplaceTempView ("SingleTable");

		//TODO Fix bug - convert to scala
		sparkSession.time(sparkSession.sql(SingleTableQueries.QUERY6).show());
	}



}
