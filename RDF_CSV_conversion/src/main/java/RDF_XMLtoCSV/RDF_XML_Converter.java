package RDF_XMLtoCSV;

import org.apache.jena.rdf.model.Model;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

public class RDF_XML_Converter {

	private String sourceFile;
	private String destinationFile;
	private JFileChooser chooser;
	private HashMap<String, String> hashMap;
	private StringBuilder sb;

	public RDF_XML_Converter(){

		this.chooser = new JFileChooser();
		this.selectSourceFile();
		this.sb = new StringBuilder();
		this.hashMap = new HashMap<>();

		sb.append("subject,object,predicate\n");

	}

	private void selectSourceFile(){

		chooser.setFileSelectionMode( JFileChooser.FILES_ONLY );
		chooser.setDialogTitle("Select Source file");
		chooser.setFileFilter(
										new FileNameExtensionFilter("RDF/XML", "rdf"));

		if( chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION ) {

			this.sourceFile =  chooser.getSelectedFile().getAbsolutePath();

			this.destinationFile = sourceFile.replace(".rdf",".csv");

		} else {
			System.out.println("No Selection ");
			System.exit(0);
		}
	}

	public void read(){

		try {
			File file = new File(sourceFile);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

			Document doc = dBuilder.parse(file);
			doc.getDocumentElement().normalize();

			NodeList universities = doc.getElementsByTagName("rdf:RDF");

			for (int temp = 0; temp < universities.getLength(); temp++) {

				Node nNode = universities.item(temp);

				NamedNodeMap predicateURIs = nNode.getAttributes();
				for (int i = 0; i < predicateURIs.getLength(); ++i)
				{
					Attr attr = (Attr) predicateURIs.item(i);
					String attrName = attr.getNodeName();
					String attrValue = attr.getNodeValue();

					hashMap.put(attrName.split(":")[1], attrValue);
				}

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element university = (Element) nNode;
					NodeList rows = university.getElementsByTagName("rdf:Description");

					build(rows);
				}

				hashMap.clear();

			}

		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void write(){

		File file = new File(destinationFile);

		try {
			Writer writer = new FileWriter(file, false);
			writer.write(sb.toString());
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void build(NodeList rows){

		for (int i = 0; i < rows.getLength(); i++) {

			Node row = rows.item(i);
			String subject = ((Element) row).getAttribute("rdf:about");

			NodeList childNodes = row.getChildNodes();

			for (int j = 0; j < childNodes.getLength(); j++) {

				Node child = childNodes.item(j);

				if(child.getNodeType() == Node.ELEMENT_NODE ){

					String[] predicate_temp = child.getNodeName().split(":");
					String predicate = hashMap.get(predicate_temp[0])+"/"+predicate_temp[1];
					String object = child.hasAttributes() ?
							  ((Element) child).getAttribute("rdf:resource")
							  : child.getTextContent();

					sb.append(subject)
							  .append(",")
							  .append(predicate)
							  .append(",")
							  .append(object)
							  .append("\n");

				}
			}
		}
	}
}
