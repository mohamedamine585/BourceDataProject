package org.medproject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import main.java.org.medproject.CompanyQuantitiesMapper;
import main.java.org.medproject.CompanyQuantityReducer;
import main.java.org.medproject.BetaMapper;
import main.java.org.medproject.BetaReducer;
import main.java.org.medproject.RsiMapper;
import main.java.org.medproject.RsiReducer;

public class Runner {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MoyenneIndicePlusHaut_Runner <input path> <output path> <WINDOW_SIZE>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
       
        Job job1 = Job.getInstance(conf, "Average of Max & Min Index Calculation");
        conf.setInt("WINDOW_SIZE", Integer.parseInt(args[2]));
  
   
        job1.setJarByClass(Runner.class);

        job1.setMapperClass(AverageMaxMinIndexMapper.class);

        job1.setReducerClass(AverageMaxMinIndexReducer.class);
      
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoublePair.class);

        Job job2 = Job.getInstance(conf, "Mobile Average Calculation");
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2, new Path(args[0]+"/indice/")); 

        job2.setJarByClass(Runner.class);
        

        job2.setMapperClass(MovingAverageMapper.class);
        
        job2.setReducerClass(MovingAverageReducer.class);
      
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

    
        
        Job job3 = Job.getInstance(conf,"Deviation calculation");
        job3.setJarByClass(Runner.class);
        

        job3.setMapperClass(DeviationMapper.class);
        
        job3.setReducerClass(DeviationReducer.class);
      
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
          
        Job job4 = Job.getInstance(conf,"Sector Analysis");
        job4.setJarByClass(Runner.class);
        

        job4.setMapperClass(SectorAnalysisMapper.class);
        
        job4.setReducerClass(SectorAnalysisReducer.class);
      
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
      
           
        Job job5 = Job.getInstance(conf,"Top Transactioners");
        job5.setJarByClass(Runner.class);
        

        job5.setMapperClass(TopTransactionersMapper.class);
        
        job5.setReducerClass(TopTransactionersReducer.class);
      
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        Job job6 = Job.getInstance(conf,"Top Transactioners");
        job6.setJarByClass(Runner.class);
        

        job6.setMapperClass(ClotureParGroupMapper.class);
        
        job6.setReducerClass(ClotureParGroupReducer.class);
      
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(DoubleWritable.class);
        Job job7 = Job.getInstance(conf,"Top Transactioners");
        job7.setJarByClass(Runner.class);
        

        job7.setMapperClass(VolatilityMapper.class);
        
        job7.setReducerClass(VolatilityReducer.class);
      
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(DoubleWritable.class);

        Job job8 = Job.getInstance(conf, "Company Quantity");
        
      

        job8.setJarByClass(Runner.class);
        

        job8.setMapperClass(CompanyQuantitiesMapper.class);
        
        job8.setReducerClass(CompanyQuantityReducer.class);
      
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(DoubleWritable.class);
        Job job9 = Job.getInstance(conf, "Company RSI");
        
      

        job9.setJarByClass(Runner.class);
        

        job9.setMapperClass(RsiMapper.class);
        
        job9.setReducerClass(RsiReducer.class);
      
        job9.setOutputKeyClass(Text.class);
        job9.setOutputValueClass(Text.class);
        Job job10 = Job.getInstance(conf, "Company Quantity");
        
      

        job10.setJarByClass(Runner.class);
        

        job10.setMapperClass(BetaMapper.class);
        
        job10.setReducerClass(BetaReducer.class);
      
        job10.setOutputKeyClass(Text.class);
        job10.setOutputValueClass(DoubleWritable.class);


      
        FileInputFormat.addInputPath(job1, new Path(args[0] +"/indice/"));
        FileInputFormat.addInputPath(job2, new Path(args[0] +"/indice/"));
        FileInputFormat.addInputPath(job3, new Path(args[0] +"/indice/"));
        FileInputFormat.addInputPath(job4, new Path(args[0] +"/cotation/"));
        FileInputFormat.addInputPath(job5, new Path(args[0] +"/cotation/"));
        FileInputFormat.addInputPath(job6, new Path(args[0] +"/cotation/"));
        FileInputFormat.addInputPath(job7, new Path(args[0] +"/cotation/"));
        FileInputFormat.addInputPath(job8, new Path(args[0] +"/cotation/"));
        FileInputFormat.addInputPath(job9, new Path(args[0] +"/cotation/"));
        FileInputFormat.addInputPath(job10, new Path(args[0] +"/cotation/"));

        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/indice/Average_of_Max_Min_Index_Calculation"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/indice/Mobile_Average_Calculation"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/indice/Deviation_calculation"));
        FileOutputFormat.setOutputPath(job4, new Path(args[1]+"/cotation/Sector_Analysis"));
        FileOutputFormat.setOutputPath(job5, new Path(args[1]+"/cotation/Top_Transactioners"));
        FileOutputFormat.setOutputPath(job6, new Path(args[1]+"/cotation/clotureparGroupe"));
        FileOutputFormat.setOutputPath(job7, new Path(args[1]+"/cotation/Volatilityofsectors"));
        FileOutputFormat.setOutputPath(job8, new Path(args[1]+"/cotation/CompanyTotalQuantities"));
        FileOutputFormat.setOutputPath(job9, new Path(args[1]+"/cotation/RSI"));
        FileOutputFormat.setOutputPath(job10, new Path(args[1]+"/cotation/BetaCotation"));
        

        System.exit((job1.waitForCompletion(true) && job2.waitForCompletion(true) && job3.waitForCompletion(true) && job4.waitForCompletion(true) && job5.waitForCompletion(true) && job6.waitForCompletion(true) && job7.waitForCompletion(true) && job8.waitForCompletion(true) && job9.waitForCompletion(true) && job10.waitForCompletion(true)) ? 0 : 1);
    }
}
