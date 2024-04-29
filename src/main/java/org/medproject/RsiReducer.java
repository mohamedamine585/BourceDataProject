package main.java.org.medproject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class RsiReducer extends Reducer<Text, Text, Text, Double> {
    private static final int PERIOD = 14;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // List to hold the prices for the specified period
        ArrayList<Double> prices = new ArrayList<>();

        // Populate the list with prices
        for (Text value : values) {
            prices.add(Double.parseDouble(value.toString()));
        }

        // Calculate gains and losses over the specified period
        List<Double> changes = calculateChanges(prices);

        // Calculate average gains and average losses over the specified period
        double averageGain = calculateAverageGain(changes);
        double averageLoss = calculateAverageLoss(changes);

        // Calculate Relative Strength (RS)
        double RS;
        if(!Double.isNaN(averageLoss) && !Double.isNaN(averageGain)){
            RS = averageGain / averageLoss;
        }
        else{
            RS = 0;
        }

        // Calculate RSI
        double RSI = 100 - (100 / (1 + RS));

        // Emitting company and RSI
        context.write(new Text(key.toString() + ","), RSI);
    }

    private List<Double> calculateChanges(List<Double> prices) {
        List<Double> changes = new ArrayList<>();
        for (int i = 1; i < prices.size(); i++) {
            changes.add(prices.get(i) - prices.get(i - 1));
        }
        return changes;
    }

    private double calculateAverageGain(List<Double> changes) {
        // Calculate average gain over the specified period
        // For simplicity, you can sum all positive changes
        return changes.stream().filter(change -> change > 0).mapToDouble(Double::doubleValue).sum() / PERIOD;
    }

    private double calculateAverageLoss(List<Double> changes) {
        // Calculate average loss over the specified period
        // For simplicity, you can sum all negative changes
        return Math.abs(changes.stream().filter(change -> change < 0).mapToDouble(Double::doubleValue).sum()) / PERIOD;
    }
}