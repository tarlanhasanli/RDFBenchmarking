package NTtoCSV;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;

public class NT_Converter {

    private String sourceFile;
    private String destinationFile;
    private JFileChooser chooser;
    private FileWriter fileWriter;

    public NT_Converter(){

        this.chooser = new JFileChooser();
        this.selectSourceFile();

	    System.out.println(this.destinationFile);

	    try {

		    fileWriter = new FileWriter(new File(this.destinationFile), false);
		    fileWriter.write("subject,predicate,object\n");

	    } catch (IOException e) {
		    e.printStackTrace();
	    }

    }

    private void selectSourceFile(){

        chooser.setFileSelectionMode( JFileChooser.FILES_ONLY );
        chooser.setDialogTitle("Select Source file");
        chooser.setFileFilter(
                new FileNameExtensionFilter("N-TRIPLES", "nt"));

        if( chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION ) {

            this.sourceFile =  chooser.getSelectedFile().getAbsolutePath();

            this.destinationFile = this.sourceFile.replaceAll("nt$","csv");

        } else {

            System.out.println("No Selection ");
            System.exit(0);

        }
    }

    public void convert() {

	    BufferedReader bufferedReader;

        try{

	        bufferedReader = Files.newBufferedReader(Paths.get(this.sourceFile));

	        String line;
	        while ((line = bufferedReader.readLine()) != null) {

	        	String[] cols = line.replaceAll(".$", "")
				        .trim()
				        .split(" ", 3);

		        fileWriter.write(normalize(cols[0])+",");
		        fileWriter.write(normalize(cols[1])+",");
		        fileWriter.write(normalize(cols[2])+",");
		        fileWriter.write("\n");

	        }

	        bufferedReader.close();
	        fileWriter.close();

        } catch (Exception e){

	        e.printStackTrace();

        }

    }

    private String normalize(String s){

	    int i = s.indexOf('^');

	    s = s.trim().replaceAll("[<>]", "");

	    return (i > 0) ? s.substring(0, i) : s;

    }

}
