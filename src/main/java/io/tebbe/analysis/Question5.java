package io.tebbe.analysis;

import io.tebbe.Index;
import io.tebbe.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ctebbe
 * Which carriers have the most delays? You should report on the total number of delayed flights
 and also the total number of minutes that were lost to delays. Which carrier has the highest average
 delay?
 */
public class Question5 {

    /*
        key = carrier code
        value = total_delay_mins
     */
    public static class MapCarrierDelay extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Text keyCarrier = new Text(fields[Index.UNIQUECARRIER]);
            IntWritable sumDelay = Utils.getSumDelay(fields);
            context.write(keyCarrier, sumDelay);
        }
    }

    /*
        key = carrier code
        value = carrier_delay \t num_delayed_flights \t avg_delay
     */
    public static class ReduceCarrierDelay extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int numDelayedFlights=0, numFlights=0, sumDelay=0;
            for(IntWritable value : values) {
                int delay = value.get();
                if(delay > 0) {
                    numDelayedFlights++;
                    sumDelay += delay;
                }
                numFlights++;
            }
            Text outValue = new Text(sumDelay + "\t" + numDelayedFlights + "\t" + (sumDelay/numFlights));
            context.write(key, outValue);
        }
    }
}
