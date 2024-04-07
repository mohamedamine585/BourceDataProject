package org.medproject;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MoyenneIndicePlusHaut_Reducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }
        double average = (double) sum / count;
        context.write(new Text("Average INDICE_PLUS_HAUT"), new DoubleWritable(average));
    }
}
