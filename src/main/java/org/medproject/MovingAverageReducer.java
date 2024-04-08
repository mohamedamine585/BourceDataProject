package org.medproject;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MovingAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {



    @Override
   public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        int WINDOW_SIZE = context.getConfiguration().getInt("WINDOW_SIZE", 10);
      
        ArrayList<Double> prices = new ArrayList<>();
        for (DoubleWritable value : values) {
            prices.add(value.get());
        }
        double sum = 0.0;
        int count = 0;
       
        for (double price : prices) {
            sum += price;
            count++;
            if (count >= WINDOW_SIZE) {
                double movingAverage = sum / WINDOW_SIZE;
               
                context.write(key, new DoubleWritable(movingAverage));
                sum -= prices.get(count - WINDOW_SIZE);
            }
        }
    }
}
