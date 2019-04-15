package com.company;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;

public class Main {

    private String sourceFile;
    private String destinationFolder;
    private JFileChooser chooser;

    public Main(){

        this.chooser = new JFileChooser();
        this.selectSourceFile();

        //Schema2 sc2 = new Schema2(sourceFile, destinationFolder + "\\VerticalTables");
        Schema3 sc3 = new Schema3(sourceFile, destinationFolder + "\\PropertyTables");

        //sc2.convert();
        sc3.convert();

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

    public static void main(String[] args) {

        new Main();

        //ReadNT rnt = new ReadNT();

        //rnt.writeData();

    }
}
