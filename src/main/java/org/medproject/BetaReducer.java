package main.java.org.medproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
public  class BetaReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
   
 private DoubleWritable beta = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sumXY = 0.0;
            double sumX = 0.0;
            double sumY = 0.0;
            double sumXX = 0.0;
            double n = 0.0;
            
            for (DoubleWritable value : values) {
                double closingPrice = value.get();
                sumXY += closingPrice * n;
                sumX += n;
                sumY += closingPrice;
                sumXX += n * n;
                n++;
            }
            
            double betaValue = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
            beta.set(betaValue);
            
            context.write(new  Text(key.toString()+","), beta);
        }
    

}