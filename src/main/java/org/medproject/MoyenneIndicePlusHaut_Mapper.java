package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MoyenneIndicePlusHaut_Mapper extends Mapper<LongWritable, Text, Text, DoublePair> {

    private Text Key = new Text("Indices");
    
       
    private final static DoublePair pair = new DoublePair();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length >= 9) { // Ensure that the line has at least 9 tokens
            try {
                String indicePlusHautStr = tokens[7].trim();
                String indicePlusBasStr = tokens[8].trim();
                if (!indicePlusHautStr.isEmpty()) {
                    double indicePlusHaut = Double.parseDouble(indicePlusHautStr);
                    double indicePlusBas = Double.parseDouble(indicePlusBasStr);
                    pair.set(indicePlusHaut, indicePlusBas);
                    context.write(Key, pair); // Emit the value
                 


                }
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }else{
           System.out.println("Missing fields");  
         }
    }
}
