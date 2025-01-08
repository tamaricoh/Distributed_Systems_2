package com.dsp;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
//import java.util.List;
import java.util.Map;
//import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class trigramListStep {
    // Mapper class to pass data as-is to the reducer
    public static class MapperClass extends Mapper<Object, Text, Text, Text> { // ==============
        private Text newVal = new Text();
        private Text newKey = new Text();
    @Override
    public void map(Object key, Text input, Context context) throws IOException, InterruptedException {
        // System.out.println("[DEBUG] STEP 4 map running");
        // String[] parts = (key.toString()).split(Defs.delimiter);
        // if (parts.length >= 3) {
        //     System.out.println("[DEBUG]  the input:" + key.toString() + " is valid");
        //     String twoWords = parts[0] + Defs.SPACE + parts[1];
        //     String valueWithThirdWord = parts[2] + Defs.SPACE + String.valueOf(input.get()) ;
        //     context.write(new Text(twoWords), new Text(valueWithThirdWord));
        // }
        // else{
        //     System.out.println("[DEBUG]  the input:" + key.toString() + " is invalid");
        // }
        String[] parts = input.toString().split("\t");
            newKey.set(parts[0]);
            newVal.set(parts[1]);
            context.write(newKey, newVal);
        }
}

public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // ArrayList<Map.Entry<String, Double>> pairs = new ArrayList<>();
        // System.out.println("[DEBUG] STEP 4 reducer is running");
        // for (Text val : values) {
        //     String[] parts = val.toString().split(Defs.SPACE);
        //     pairs.add(new AbstractMap.SimpleEntry<>(parts[0], Double.parseDouble(parts[1])));
        // }
        
        // Collections.sort(pairs, (a, b) -> Double.compare(b.getValue(), a.getValue()));
        
        // for (Map.Entry<String, Double> pair : pairs) {
        //     context.write(key, new Text(pair.getKey() + " " + String.valueOf(pair.getValue())));
        // }

        String output = "";
        for (Text val : values){
            output += "##" + val.toString();
        }
        context.write(key, new Text(output));
    }
}

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args[0]);
        System.out.println(args[1]);
        System.out.println(args[2]);
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[0]);
        job.setJarByClass(trigramListStep.class);

        // Set Mapper, Reducer, and other job parameters
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        // job.setInputFormatClass(SequenceFileInputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);
        // Set input and output paths

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.out.println("[DEBUG] STEP 4 job started!");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
