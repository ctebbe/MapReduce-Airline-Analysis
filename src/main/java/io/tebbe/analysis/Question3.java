package io.tebbe.analysis;

import io.tebbe.Index;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ctebbe
 * What are the major hubs (busiest airports) in continental U.S.? Please list the top 10. Has there
 been a change over the 21-year period covered by this dataset?
 */
public class Question3 {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Text keyYearAirport = new Text(fields[Index.YEAR] + fields[Index.ORIGIN]);
            context.write(keyYearAirport , new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }
}
