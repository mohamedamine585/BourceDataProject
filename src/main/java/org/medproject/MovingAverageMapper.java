package org.medproject;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MovingAverageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
   
     
            String[] tokens = value.toString().split(",");
          
            if (tokens.length >= 5  ) { 
            String Index = tokens[4].trim();
           
                
                if(!Index.equals("INDICE_JOUR") ){
                    String date = tokens[0].trim();
              
                    double closePrice = Double.parseDouble(Index); 
              
                    context.write(new Text(date), new DoubleWritable(closePrice));
                }
            
                
                
            }
       
      
    }
}
