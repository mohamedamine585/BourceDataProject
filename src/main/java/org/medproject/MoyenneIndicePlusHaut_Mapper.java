package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MoyenneIndicePlusHaut_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length >= 9) { // Ensure that the line has at least 9 tokens
            try {
                String indicePlusHautStr = tokens[7].trim();
                if (!indicePlusHautStr.isEmpty()) {
                    double indicePlusHaut = Double.parseDouble(indicePlusHautStr);
                    outKey.set("average"); // Set a constant key for all records
                    context.write(outKey, new IntWritable((int)indicePlusHaut)); // Emit the value
                }
            } catch (NumberFormatException e) {
                // Handle parsing errors
            }
        }
    }
}
