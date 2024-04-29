package main.java.org.medproject;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.lang.*;

    public class CompanyQuantitiesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable quantity = new DoubleWritable();
        private final Text company = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(","); // Splitting the input line by comma
            String companyValue = tokens[3];
            double quantityTraded = Double.parseDouble(tokens[8]); // Assuming QUANTITE_NEGOCIEE is at index 8

            company.set(companyValue);
            quantity.set(quantityTraded);

            // Emitting company and quantity as key-value pair
            context.write(company, quantity);
        }
    }
