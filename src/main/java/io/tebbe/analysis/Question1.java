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
 * what is the best time of day/day of week/time of year to fly to min delays?
 */
public class Question1 {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Text keyTime = new Text(
                    fields[Index.DEPTIME] + "," +
                    fields[Index.DAYOFWEEK] + "," +
                    fields[Index.MONTH]);
            IntWritable sumDelay = Utils.getSumDelay(fields);
            context.write(keyTime, sumDelay);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sumDelay = 0;
            for(IntWritable delay : values) {
                sumDelay += delay.get();
            }
            context.write(new Text(String.valueOf(sumDelay)), key);
        }
    }
}
