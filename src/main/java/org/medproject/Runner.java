package org.medproject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
        TextInputFormat.addInputPath(job2, new Path(args[0])); 

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
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1],"output_job1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1],"output_job2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1],"output_job3"));

        System.exit((job1.waitForCompletion(true) && job2.waitForCompletion(true) && job3.waitForCompletion(true)) ? 0 : 1);
    }
}
