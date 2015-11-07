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
 * Do older planes cause more delays? Include details to substantiate your analysis.
 * use tailnum to find manufacture date
 */
public class Question6 {

    /*
        key = tail num
        value = sum_delays
     */
    public static class MapTailNumDelay extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Text keyTailNum = new Text(fields[Index.TAILNUM]);
            IntWritable sumDelay = Utils.getSumDelay(fields);
            context.write(keyTailNum, sumDelay);
        }
    }

    /*
        key = tail num
        value = sum tail num delays
     */
    public static class ReduceTailNumDelay extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new Text(sum+""));
        }
    }

    /*
        key = tail num
        value = sum tail num delays
     */
    public static class MapTailNumDelay2 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\t");
            Text keyAirport = new Text(fields[0]);
            Text delay = new Text(String.valueOf(Utils.getInt(fields[1])));
            context.write(keyAirport, delay);
        }
    }

    /*
        key = tail num
        value = manufactured year
     */
    public static class MapCities extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Text keyTailNum = new Text(fields[0]);
            Text year = new Text(fields[8]+";");
            context.write(keyTailNum, year);
        }
    }

    /*
        key = city
        value = weather delay
     */
    public static class ReduceCitiesDelay extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text year=null, delay = null;
            for(Text value : values) {
                if(value.toString().contains(";")) { // year
                    year = new Text(value.toString().substring(0, value.getLength()));
                } else {
                    delay = new Text(value);
                }
            }
            if(year!=null && delay!=null)
                context.write(year, delay);
       }
    }
}
