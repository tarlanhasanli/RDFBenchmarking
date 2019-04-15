import CSVtoParquet.Parquet_Converter;
import RDF_XMLtoCSV.RDF_XML_Converter;

public class Main {


    public Main(){

        //new NT_Converter().read().write();

        //new Avro_Converter();

        //new Parquet_Converter();
    }

    public static void main(String[] args){

        // new Main();

        RDF_XML_Converter rdf_xml_converter = new RDF_XML_Converter();

        rdf_xml_converter.read();
        rdf_xml_converter.write();

    }


}
