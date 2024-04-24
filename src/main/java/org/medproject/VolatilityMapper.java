package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

public class VolatilityMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        // Assuming the price data is in CSV format and the price change is in the second column
        double priceChange = Double.parseDouble(tokens[1]);
        context.write(new Text("volatility"), new DoubleWritable(priceChange));
    }
} 
