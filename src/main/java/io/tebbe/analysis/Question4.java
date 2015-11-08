package io.tebbe.analysis;

import io.tebbe.Index;
import io.tebbe.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ctebbe
 * Which cities experience the most weather related delays? Please list the top 10
 */
public class Question4 {

    /*
        key = airport code
        value = weather delay
     */
    public static class Map0 extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Text keyAirport = new Text(fields[Index.ORIGIN]);
            IntWritable delay = new IntWritable(Utils.getInt(fields[Index.WEATHERDELAY]));
            context.write(keyAirport, delay);
        }
    }

    /*
        key = airport code
        value = summed weather delay
     */
    public static class Reduce0 extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    /*
        key = airport code
        value = weather delay
     */
    public static class Map0_1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            Text keyAirport = new Text(fields[0]);
            Text delay = new Text(fields[1]);
            context.write(keyAirport, delay);
        }
    }

    /*
        key = airport code
        value = city
     */
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Text keyAirportCode = new Text(fields[0]);
            Text airport = new Text(fields[2]);
            context.write(keyAirportCode, airport);
        }
    }

    /*
        key = city
        value = weather delay
     */
    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text keyCity = null;
            List<Text> delays = new ArrayList<Text>();
            for(Text value : values) {
                if(Utils.isIntegerValue(value))
                    delays.add(new Text(value));
                else
                    keyCity = new Text(value);
            }

            if(keyCity == null)
                keyCity = key;

            for(Text delay : delays)
                context.write(keyCity, delay);
        }
    }
}
