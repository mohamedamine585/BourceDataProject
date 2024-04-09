package org.medproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DeviationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        List<Double> indexList = new ArrayList<>();
        double sum = 0.0;
        int count = 0;

        for (DoubleWritable value : values) {
            double index = value.get();
            indexList.add(index);
            sum += index;
            count++;
        }

        double mean = sum / count;
        double sumSquaredDiff = 0.0;

        for (double index : indexList) {
            sumSquaredDiff += Math.pow(index - mean, 2);
        }

        double standardDeviation = Math.sqrt(sumSquaredDiff / count);
        context.write(key, new DoubleWritable(standardDeviation));
    }
}

