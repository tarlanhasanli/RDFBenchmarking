package NTtoCSV;

import java.io.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;

public class Read_NT {

    private String sourceFile;
    private String destinationFile;
    private Model model;
    private JFileChooser chooser;

    public Read_NT(){

        this.chooser = new JFileChooser();
        this.selectSourceFile();

    }

    private void selectSourceFile(){

        chooser.setFileSelectionMode( JFileChooser.FILES_ONLY );
        chooser.setDialogTitle("Select Source file");
        chooser.setFileFilter(
                new FileNameExtensionFilter("N-TRIPLES", "nt"));

        if( chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION ) {

            this.sourceFile =  chooser.getSelectedFile().getAbsolutePath();

            this.destinationFile = chooser
                    .getCurrentDirectory()
                    .getAbsolutePath()
                    .replaceAll("nt$","csv");

        } else {

            System.out.println("No Selection ");
            System.exit(0);

        }
    }

    public Read_NT read() {

        this.model = ModelFactory.createDefaultModel();

        try{

            InputStream inputStream = FileManager.get().open(sourceFile);

            if(inputStream == null)
                System.out.println("File not specified or not exists!");
            else
                model = model.read(inputStream, null, "N-TRIPLE");

        } catch (Exception e){

            System.err.println("File not specified or not exists!: " + e.getMessage());

        }

        return this;

    }

    public void write(){

        if(model != null)
            try {
                model.write( new FileWriter(destinationFile));
            } catch (Exception e) {
                e.printStackTrace();
            }
        else
            System.out.println("RDF Model is not specified");

    }


}
