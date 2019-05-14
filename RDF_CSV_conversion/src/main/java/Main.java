import CSVtoHDFS.*;
import NTtoCSV.NT_Converter;
import SchemaConverter.SchemaConverter;

import java.io.File;

public class Main {

    public Main(){

//	    NT_Converter nt_converter = new NT_Converter();
//	    nt_converter.convert();

//	    SchemaConverter sc = new SchemaConverter();

        try {
            HDFS_Converter hdfs_converter =
                    new HDFS_Converter("C:\\Users\\Tarlan Hasanli\\Documents\\2019 spring\\Big Data Management\\Project\\RDFBenchmarking\\Datasets\\CSV");

            hdfs_converter.config()
                    .read()
                    .convert(HDFS_type.ORC);
//                    .convert(HDFS_type.AVRO)
//                    .convert(HDFS_type.PARQUET);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args){

        new Main();

    }


}
