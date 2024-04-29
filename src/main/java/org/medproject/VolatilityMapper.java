package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

public class VolatilityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text sector = new Text();
    private DoubleWritable price = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        
        // Ensure that the tokens array has at least 10 elements
        if (tokens.length < 5) {
            return;
        }
        
        String sectorName = tokens[1];
       String stockPrice = tokens[4];
        if( Utils.isDouble(stockPrice)){
            sector.set(sectorName);
            price.set(Double.parseDouble(stockPrice));
            context.write(sector, price);
        }
       
    }
} 
