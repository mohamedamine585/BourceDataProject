package org.medproject;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DeviationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text filename = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length >= 5) {
            String fileNameString = ((FileSplit) context.getInputSplit()).getPath().getName();
        fileNameString = fileNameString.substring(0,fileNameString.length()-4 ); // remove .csv from file name
            filename.set(fileNameString);
      
             String indiceVeille = tokens[4].trim();
             double index = 0;
            if(!indiceVeille.equals("INDICE_VEILLE")){
                
              index = Double.parseDouble(indiceVeille);
               
            }
          
            context.write(filename, new DoubleWritable(index));
           
        }
    }
}