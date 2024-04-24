package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TopTransactionersMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        
        // Ensure that the tokens array has at least 10 elements
        if (tokens.length < 10) {
            return;
        }
        
        // Extract the transactioner identifier from tokens[3] (assuming it uniquely identifies transactioners)
        String transactionerId = tokens[3].trim(); // Assuming tokens[3] contains the transactioner ID
        
        // Validate if tokens[9] is an integer
        if (!Utils.isInteger(tokens[9].trim())) {
            return;
        }

        // Parse the number of transactions
        int numberOfTransactions = Integer.parseInt(tokens[9].trim());

        // Emit the transactionerId as the key and numberOfTransactions as the value
        context.write(new Text(transactionerId), new IntWritable(numberOfTransactions));
    }
}
