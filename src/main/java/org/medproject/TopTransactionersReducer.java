package org.medproject;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTransactionersReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, Text> sortedTransactioners = new TreeMap<>(Comparator.reverseOrder());

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer totalTransactions = 0;

        // Summing up the transaction values for the key
        for (IntWritable value : values) {
            totalTransactions += value.get();
        }

        // Add the key-value pair to the TreeMap for sorting
        sortedTransactioners.put(totalTransactions, new Text(key));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the sorted transactioners
        for (Map.Entry<Integer, Text> entry : sortedTransactioners.entrySet()) {
            context.write(entry.getValue(), new IntWritable(entry.getKey()));
        }
    }
}
