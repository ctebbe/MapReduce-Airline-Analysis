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
    public static class Map0 extends Mapper<LongWritable, Text, Text, IntWritable> {
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
    public static class Reduce0 extends Reducer<Text, IntWritable, Text, Text> {
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
            Text outValue = new Text(sumDelay + "," + numDelayedFlights + "," + (sumDelay/numFlights));
            context.write(key, outValue);
        }
    }

    public static class Map0_1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] carrierAndPayload = value.toString().split("\\t");
            context.write(new Text(carrierAndPayload[0]), new Text(carrierAndPayload[1]));
        }
    }

    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            context.write(new Text(fields[0]), new Text(fields[1] +", "));
        }
    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values) {
            }
        }
    }
}
