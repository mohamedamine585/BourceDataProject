package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

public class VolatilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double totalChange = 0;
        int count = 0;
        for (DoubleWritable value : values) {
            totalChange += value.get();
            count++;
        }
        double averageChange = totalChange / count;
        context.write(key, new DoubleWritable(averageChange));
    }
}