package io.tebbe.analysis;

import io.tebbe.Index;
import io.tebbe.Utils;
import org.apache.avro.generic.GenericData;
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
 * What are the major hubs (busiest airports) in continental U.S.? Please list the top 10. Has there
 been a change over the 21-year period covered by this dataset?
 */
public class Question3 {

    public static class Map0 extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            Text keyYearAirport = new Text(fields[Index.ORIGIN] +","+ fields[Index.YEAR]);
            context.write(keyYearAirport , new IntWritable(1));
        }
    }

    public static class Reduce0 extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            context.write(new Text(String.valueOf(sum)), key);
        }
    }

    public static class Map0_1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] delayAndKey = value.toString().split("\\t");
            String[] airportAndYear = delayAndKey[1].split(",");
            context.write(new Text(airportAndYear[0]), new Text(airportAndYear[1] +","+ delayAndKey[0]));
        }
    }

    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            context.write(new Text(values[0].replace("\"", "").trim()), new Text(values[1].replace("\"", "").trim()+", "));
        }
    }

    /*
    ABE \t Lehigh airport
    ABE \t 1987,1258
    ...
     */
    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text airportKey = null;
            List<Text> lValues = new ArrayList<Text>();
            for(Text value : values) {
                if(Utils.isIntegerValue(value.toString().split(",")[1])) {
                    lValues.add(new Text(value.toString()));
                } else {
                    airportKey = new Text(value.toString().split(",")[0]);
                }
            }

            if(airportKey != null)
                for(Text value : lValues) {
                    context.write(airportKey, value);
                }
        }
    }

    public static class MapSort extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] airportAndPayload = value.toString().split("\\t");
            String[] yearAndFlights = airportAndPayload[1].split(",");
            context.write(new Text(yearAndFlights[1]), new Text(yearAndFlights[0] +","+ airportAndPayload[0]));
        }
    }
}
