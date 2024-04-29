package main.java.org.medproject;
import java.io.IOException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

     public  class CompanyQuantityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable totalQuantity = new DoubleWritable();

        public void reduce(javax.xml.soap.Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            totalQuantity.set(sum);

            // Emitting company and total quantity traded
            context.write(new Text(key.toString() + ","), totalQuantity);
        }
    
}
