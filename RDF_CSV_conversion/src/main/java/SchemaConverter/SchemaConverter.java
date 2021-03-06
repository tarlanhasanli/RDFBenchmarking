package SchemaConverter;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;

public class SchemaConverter {

    private String sourceFile;
    private String destinationFolder;
    private JFileChooser chooser;

    public SchemaConverter(){

        this.chooser = new JFileChooser();
        this.selectSourceFile();

        new Schema2(sourceFile, destinationFolder + "\\VerticalTables").convert();
        new Schema3(sourceFile, destinationFolder + "\\PropertyTables").convert();

    }

    private void selectSourceFile(){

        chooser.setFileSelectionMode( JFileChooser.FILES_ONLY );
        chooser.setDialogTitle("Select Source file");
        chooser.setFileFilter(new FileNameExtensionFilter(
                "CSV file", "csv"));

        if( chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION ) {

            this.sourceFile =  chooser.getSelectedFile().getAbsolutePath();
            this.destinationFolder = chooser
                    .getCurrentDirectory()
                    .getParent();

        } else {

            System.out.println("No Selection ");
            System.exit(0);

        }
    }

}
