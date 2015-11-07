package io.tebbe;

import io.tebbe.analysis.Question1;
import io.tebbe.analysis.Question3;
import io.tebbe.results.SortByValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Created by ctebbe.
 */
public class Main {

    private static final NumberFormat nf = new DecimalFormat("00");
    public static final String HOST = "hdfs://salt-lake-city";
    public static final int PORT = 32401;

    private static final String FILENAME = "/part-r-00000";
    private static final String MAINDATA = "/data/main";


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Main main = new Main();
        Configuration conf = new Configuration();
        long secs = 1800000;
        conf.setLong("mapred.task.timeout", secs);

        //main.runQuestion1_2(conf);
        main.runQuestion3(conf);
    }

    private void runQuestion3(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        Job question3 = Job.getInstance(conf);
        question3.setJarByClass(Question3.class);
        question3.setOutputKeyClass(Text.class);
        question3.setOutputValueClass(Text.class);

        question3.setMapperClass(Question3.Map0.class);
        question3.setMapOutputValueClass(IntWritable.class);
        question3.setInputFormatClass(TextInputFormat.class);

        question3.setReducerClass(Question3.Reduce0.class);
        question3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputDirRecursive(question3, true);
        FileInputFormat.addInputPath(question3, new Path(MAINDATA+"/1987.csv"));
        FileOutputFormat.setOutputPath(question3, new Path("/home/question3"));
        question3.waitForCompletion(true);

        Job airportsCityJob = Job.getInstance(conf);
        airportsCityJob.setJarByClass(Question3.class);
        airportsCityJob.setOutputKeyClass(Text.class);
        airportsCityJob.setOutputValueClass(Text.class);

        airportsCityJob.setMapperClass(Question3.Map0_1.class);
        airportsCityJob.setMapperClass(Question3.Map1.class);
        airportsCityJob.setMapOutputValueClass(Text.class);

        airportsCityJob.setReducerClass(Question3.Reduce1.class);
        airportsCityJob.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(airportsCityJob, new Path("/home/question3/"+FILENAME), TextInputFormat.class, Question3.Map0_1.class);
        MultipleInputs.addInputPath(airportsCityJob, new Path("/data/supplementary/airports.csv"), TextInputFormat.class, Question3.Map1.class);
        FileOutputFormat.setOutputPath(airportsCityJob, new Path("/home/q3_results"));
        airportsCityJob.waitForCompletion(true);
    }

    private void runQuestion1_2(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        Job question1 = Job.getInstance(conf);
        question1.setJarByClass(Question1.class);
        question1.setOutputKeyClass(Text.class);
        question1.setOutputValueClass(Text.class);

        question1.setMapperClass(Question1.Map.class);
        question1.setMapOutputValueClass(IntWritable.class);
        question1.setInputFormatClass(TextInputFormat.class);

        question1.setReducerClass(Question1.Reduce.class);
        question1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputDirRecursive(question1, true);
        FileInputFormat.addInputPath(question1, new Path(MAINDATA)); //+"/1987.csv"));
        FileOutputFormat.setOutputPath(question1, new Path("/home/question1"));
        question1.waitForCompletion(true);

        Job sortJob = Job.getInstance(conf);
        sortJob.setJarByClass(SortByValue.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setMapperClass(SortByValue.MapSort.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setInputFormatClass(TextInputFormat.class);

        sortJob.setReducerClass(SortByValue.ReduceSort.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(sortJob, new Path("/home/question1/"+FILENAME));
        FileOutputFormat.setOutputPath(sortJob, new Path("/home/q1_q2_results"));
        sortJob.waitForCompletion(true);
    }

    private void runResultCollector(String iPath, String vPath, String outPath, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        Job sortJob = Job.getInstance(conf);
        sortJob.setJarByClass(SortByValue.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setMapperClass(SortByValue.MapSort.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setInputFormatClass(TextInputFormat.class);

        sortJob.setReducerClass(SortByValue.ReduceSort.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(sortJob, new Path(iPath));
        FileOutputFormat.setOutputPath(sortJob, new Path(outPath));
        sortJob.waitForCompletion(true);
        /*
        String tmp = "/tmpR";
        Job resultsJob = Job.getInstance(conf);
        resultsJob.setJarByClass(SortByValue.class);
        resultsJob.setOutputKeyClass(Text.class);
        resultsJob.setOutputValueClass(Text.class);

        resultsJob.setMapperClass(SortByValue.MapIndex.class);
        resultsJob.setMapperClass(SortByValue.MapVector.class);
        resultsJob.setMapOutputValueClass(Text.class);
        //multiplyMatrixVectorJob.setInputFormatClass(TextInputFormat.class);

        resultsJob.setReducerClass(SortByValue.Reduce.class);
        resultsJob.setOutputFormatClass(TextOutputFormat.class);

        //FileInputFormat.addInputPath(multiplyMatrixVectorJob, new Path(mPath));
        MultipleInputs.addInputPath(resultsJob, new Path(iPath), TextInputFormat.class, SortByValue.MapIndex.class);
        MultipleInputs.addInputPath(resultsJob, new Path(vPath), TextInputFormat.class, SortByValue.MapVector.class);
        FileOutputFormat.setOutputPath(resultsJob, new Path(tmp));
        resultsJob.waitForCompletion(true);
        */
    }
}
