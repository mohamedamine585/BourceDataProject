package org.medproject;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AverageMaxMinIndexMapper extends Mapper<LongWritable, Text, Text, DoublePair> {

    private Text Key  = new Text();
    
       
    private   DoublePair pair = new DoublePair();
    private boolean skipFirstLine = true;



  
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         
        if (skipFirstLine) {
            skipFirstLine = false;
            return; // Skip processing the first line
        }
        String fileNameString = ((FileSplit) context.getInputSplit()).getPath().getName();
        fileNameString = fileNameString.substring(0,fileNameString.length()-4 ); // remove .csv from file name
        Key.set(fileNameString);
        String[] tokens = value.toString().split(",");

        

        if (tokens.length >= 8) { 
            try {
                String indicePlusHautStr = tokens[6].trim();
                String indicePlusBasStr = tokens[7].trim();
                double indicePlusHaut = 0;
                double indicePlusBas = 0;
                if (!indicePlusHautStr.isEmpty() && !indicePlusBasStr.isEmpty() && !indicePlusHautStr.equals("INDICE_PLUS_HAUT")) {
                 
                  
                     indicePlusHaut = Double.parseDouble(indicePlusHautStr);
                     indicePlusBas = Double.parseDouble(indicePlusBasStr);
                
                 


                }else if(indicePlusHautStr.isEmpty() && !indicePlusBasStr.isEmpty()){
                   indicePlusBas = Double.parseDouble(indicePlusBasStr);
                
                }
                pair.set(indicePlusHaut, indicePlusBas);
                context.write(Key, pair); // Emit the value
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }else if(tokens.length >= 7 ){
            String indicePlusHautStr = tokens[6].trim();
            double indicePlusHaut = 0;
            if(!indicePlusHautStr.equals("INDICE_PLUS_HAUT")){
                 indicePlusHaut = Double.parseDouble(indicePlusHautStr);
                
             
            }
            pair.set(indicePlusHaut, 0);
            context.write(Key, pair);
            

           System.out.println("Missing fields");  
         }
    }
}
