package main.java.org.medproject;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public  class RsiMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text company = new Text();
    private final Text price = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(","); // Splitting the input line by comma
        String companyValue = tokens[3]; // Assuming CODE corresponds to the company (VALEUR)
        String priceValue = tokens[4]; // Assuming VALEUR is at index 3

        company.set(companyValue);
        price.set(priceValue);

        // Emitting company and price as key-value pair
        context.write(company, price);
    }
}