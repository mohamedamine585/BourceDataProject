package org.medproject;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MoyenneIndicePlusHaut_Reducer extends Reducer<Text, DoublePair, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoublePair> values, Context context) throws IOException, InterruptedException {
        int sumIndicePlusHaut = 0;
        int sumIndicePlusBas = 0;
        int count = 0;
        for (DoublePair value : values) {
            sumIndicePlusHaut += value.getfirst();
            sumIndicePlusBas += value.getsecond();

            count++;
        }
        double MoyenneIndicePlusHaut = (double) sumIndicePlusHaut / count;
        double MoyenneIndicePlusBas = (double) sumIndicePlusBas / count;
        context.write(new Text("Moyenne INDICE_PLUS_HAUT"), new DoubleWritable(MoyenneIndicePlusHaut));
         context.write(new Text("Moyenne INDICE_PLUS_BAS"), new DoubleWritable(MoyenneIndicePlusBas));
    }
}
