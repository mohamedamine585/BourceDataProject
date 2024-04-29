package org.medproject;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClotureParGroupReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    
        int sumcloture = 0;
        for (DoubleWritable value : values) {
           sumcloture += value.get();
        
        }
   
       
        
         context.write(new Text(key.toString() + ","), new DoubleWritable(sumcloture));
    }
}
