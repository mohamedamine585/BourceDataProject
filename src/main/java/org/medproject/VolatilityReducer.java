package org.medproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

public class VolatilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
    throws IOException, InterruptedException {
List<Double> prices = new ArrayList<>();
for (DoubleWritable value : values) {
    prices.add(value.get());
}
double[] returns = calculateReturns(prices);
double volatility = calculateVolatility(returns);
if (!Double.isNaN(volatility)) {
    context.write(new Text(key.toString() + ","), new DoubleWritable(volatility));
} else {
    // Handle the case when volatility calculation results in NaN
    context.write(new Text(key.toString() + ","), new DoubleWritable(0.0));
}
}

private double[] calculateReturns(List<Double> prices) {
int n = prices.size();
double[] returns = new double[n - 1];
for (int i = 0; i < n - 1; i++) {
    double priceToday = prices.get(i);
    double priceTomorrow = prices.get(i + 1);
    if (priceToday != 0.0) {
        returns[i] = (priceTomorrow - priceToday) / priceToday;
    } else {
        returns[i] = 0.0; // Avoid division by zero
    }
}
return returns;
}

private double calculateVolatility(double[] returns) {
double sum = 0.0;
int count = 0; // Count non-NaN returns
for (double d : returns) {
    if (!Double.isNaN(d)) {
        sum += d;
        count++;
    }
}
if (count == 0) {
    return 0.0; // Return 0 if there are no valid returns
}
double mean = sum / count;
double sumOfSquaredDifferences = 0.0;
for (double d : returns) {
    if (!Double.isNaN(d)) {
        sumOfSquaredDifferences += (d - mean) * (d - mean);
    }
}
return Math.sqrt(Math.abs(sumOfSquaredDifferences)/ count);
}
}