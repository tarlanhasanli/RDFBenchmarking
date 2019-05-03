import CSVtoHDFS.*;
import NTtoCSV.NT_Converter;
import RDF_XMLtoCSV.RDF_XML_Converter;

public class Main {


    public Main(){

	    NT_Converter nt_converter = new NT_Converter();
	    nt_converter.convert();

        try {
            HDFS_Converter hdfs_converter =
                    new HDFS_Converter("/home/cloudera/RDFBenchmarking/Datasets/CSV");

            hdfs_converter.config()
                    .read()
                    .convert(HDFS_type.ORC)
                    .convert(HDFS_type.AVRO)
                    .convert(HDFS_type.PARQUET);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args){

        new Main();

        RDF_XML_Converter rdf_xml_converter = new RDF_XML_Converter();

        rdf_xml_converter.read();
        rdf_xml_converter.write();

    }


}
