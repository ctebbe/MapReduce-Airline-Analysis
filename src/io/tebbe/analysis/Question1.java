package io.tebbe.analysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ctebbe
 */
public class Question1 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] nameAndLink = value.toString().split("\\t");
            Text linkKey = new Text(nameAndLink[0]);
            Text rank = new Text(nameAndLink[1]);
            context.write(rank, linkKey);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values) {
                context.write(value, key);
            }
        }
    }
}
