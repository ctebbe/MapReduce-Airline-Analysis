package io.tebbe.results;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ctebbe
 */
public class SortByValue {
    public static class MapSort extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueAndPayload = value.toString().split("\\t");
            Text valueKey = new Text(valueAndPayload[0]);
            Text payload = new Text(valueAndPayload[1]);
            context.write(valueKey, payload);
        }
    }

    public static class ReduceSort extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values) {
                context.write(key, value);
            }
        }
    }
}
