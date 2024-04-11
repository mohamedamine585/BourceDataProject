package org.medproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
public class SectorAnalysisReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private Text outputValue = new Text();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<Double> prices = new ArrayList<>();
        for (DoubleWritable value : values) {
            prices.add(value.get());
        }
        
        // Calculate average closing price
        double sum = 0.0;
        int count = 0;
        for (double price : prices) {
            sum += price;
            count++;
        }
        double average = (count > 0) ? sum / count : 0.0;

        // Calculate standard deviation
        double variance = 0.0;
        for (double price : prices) {
            variance += Math.pow(price - average, 2);
        }
        double stdDev = Math.sqrt(variance / count);

        // Sort closing prices to calculate rate of return
        Comparator<Double> c = new Comparator<Double>(){
          
            @Override
            public int compare(Double o1, Double o2) {
                if(o1 > o2){
                    return 0;
                }
                return 1;
            };
            

        } ;
         prices.sort(c);


         
        double rateOfReturn = (count > 1) ? (prices.get(count - 1) - prices.get(0)) / prices.get(0) : 0.0;

        // Prepare output value
        String output = "Average Closing Price: " + average + ", Standard Deviation: " + stdDev + ", Rate of Return: " + rateOfReturn;
        outputValue.set(output);
        
        // Output sector along with metrics
        context.write(key, outputValue);
    }
}
