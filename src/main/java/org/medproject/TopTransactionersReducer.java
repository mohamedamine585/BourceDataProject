package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

public class TopTransactionersReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalTransactions = 0;
        int count = 0;
     
        for (IntWritable value : values) {
            if(value.get() != 0){
                totalTransactions += value.get();
                count++;
            }
            
         
            
        }
        

        context.write(key, new DoubleWritable(totalTransactions));
        Text averagekey = new Text("Average of transactions for ");
        averagekey.append(key.getBytes(), 0, key.getBytes().length);
        if(count != 0){
            context.write(averagekey,new DoubleWritable(totalTransactions/count));
        }
       
        
    }
}
