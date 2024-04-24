package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
public class TopTransactionersMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
        String[] tokens = value.toString().split(",");
        if(tokens.length < 10){
            return;
        }
         if(!Utils.isInteger(tokens[9])){
           return;
         }
        Integer NbTransactions = Integer.parseInt(tokens[9]);
        context.write(new Text(tokens[3]), new IntWritable(NbTransactions));
    }
    
}
