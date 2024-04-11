package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

public  class SectorAnalysisMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text sector = new Text();
    private DoubleWritable price = new DoubleWritable();
    private boolean skipFirstLine = true;



    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if (skipFirstLine) {
            skipFirstLine = false;
            return; // Skip processing the first line
        }
        String[] tokens = value.toString().split(",");
        if (tokens.length >= 10) {
            
            sector.set(tokens[2]); // Assuming sector is in the third column
            double closingPrice =0; 
            if(Utils.isDouble(tokens[5])){
                closingPrice = Double.parseDouble(tokens[5]); // Assuming closing price is in the sixth column
            }
     
            price.set(closingPrice);
            context.write(sector, price);
        }
    }
}

