package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

public class MovingAverageMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {

    private boolean skipFirstLine = true;



  

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
   
    
        if (skipFirstLine) {
            skipFirstLine = false;
            return; // Skip processing the first line
        }
            
            String[] tokens = value.toString().split(",");
            
            if (tokens.length >= 4  ) { 
            String Index = tokens[3].trim();
           
                
             
                    String date = tokens[0].trim();
                    double closePrice = 0;
                    if(Utils.isDouble(Index)){
                      closePrice = Double.parseDouble(Index); 
                    }
                     
              
                  
              
                    context.write(new Text(date), new DoubleWritable(closePrice));
            
            
                
                
            }
        
       
      
    }
 
}
