package main.java.org.medproject;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class BetaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  private Text company = new Text();
        private DoubleWritable closingPrice = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            
            // Assuming the column names provided are: SEANCE,GROUPE,CODE,VALEUR,OUVERTURE,CLOTURE,PLUS_BAS,PLUS_HAUT,QUANTITE_NEGOCIEE,NB_TRANSACTION,CAPITAUX
            String companyName = tokens[3];
            double closing = Double.parseDouble(tokens[5]);
            
            company.set(companyName);
            closingPrice.set(closing);

            if(companyName != "VALEUR"){
              context.write(company, closingPrice);
            }
            
            
        }
}
