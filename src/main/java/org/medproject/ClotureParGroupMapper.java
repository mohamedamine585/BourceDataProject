package org.medproject;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClotureParGroupMapper  extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    private Text Key  = new Text();
    
       



    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
        try {
        
        String[] tokens = value.toString().split(",");

        

        if (tokens.length >= 8) { 
        
                String Groupe = tokens[1].trim();
                String cloturestr= tokens[5].trim();
                double cloture = 0;
              
                if (!cloturestr.isEmpty() && !Groupe.isEmpty() && !cloturestr.equals("CLOTURE")) {
                 
                  
                     cloture = Double.parseDouble(cloturestr);
                
                
                     context.write(new Text(Groupe), new DoubleWritable(cloture) ); // Emit the value


                }
            }
           
            } catch (NumberFormatException e) {
                // Handle parsing errors
            
        
          
            

         }
    }
}
