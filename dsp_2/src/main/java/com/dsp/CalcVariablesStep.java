package com.dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
// import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class CalcVariablesStep {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable count = new IntWritable();
        private final Set<String> stopWords = new HashSet<>();
        private static final int MAX_MAP_SIZE = 1000;
        private static int num;


    protected void setup(Context context) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(Defs.stopWordsFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line.trim());
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        boolean localAggregationCommand = Defs.localAggregationCommand;
        Map<String, Integer> localAggregation = new HashMap<>();
        String[] parts = value.toString().split(Defs.TAB);
        String[] words;
        
    
        if (parts.length < 4) {
            return;
        }

        words = parts[0].toString().split(Defs.SPACE);

        if (words.length < 3) {
            return;
        }

        String w1 = words[0];
        String w2 = words[1];
        String w3 = words[2];

        try {
            num = Integer.parseInt(parts[2]); // Frequency count of the trigram
        } catch (NumberFormatException e) {
            // Skip lines with invalid count
            return;
        }

        if (stopWords.contains(w1) || stopWords.contains(w2) || stopWords.contains(w3)){
            return;
        }

        // countOccurrence("c0", localAggregation, context, localAggregationCommand, 3*num);

        countOccurrence(w1 + Defs.delimiter + Defs.astrix + Defs.delimiter + Defs.astrix, localAggregation, context, localAggregationCommand, num);

        countOccurrence(w2 + Defs.delimiter + Defs.astrix + Defs.delimiter + Defs.astrix, localAggregation, context, localAggregationCommand, num);

        countOccurrence(w3 + Defs.delimiter + Defs.astrix + Defs.delimiter + Defs.astrix, localAggregation, context, localAggregationCommand, num);

        countOccurrence(w1 + Defs.delimiter + w2 + Defs.delimiter + Defs.astrix, localAggregation, context, localAggregationCommand, num);

        countOccurrence(w2 + Defs.delimiter + w3 + Defs.delimiter + Defs.astrix, localAggregation, context, localAggregationCommand, num);

        countOccurrence(w1 + Defs.delimiter + w2 + Defs.delimiter + w3, localAggregation, context, localAggregationCommand, num);

    
        if (localAggregationCommand && localAggregation.size() >= MAX_MAP_SIZE) {
            context.setStatus("[DEBUG] Flushing local aggregation due to memory limit.");
            flushLocalAggregation(localAggregation, context);
            localAggregation.clear();
        }

    }
    
    private void countOccurrence(String key, Map<String, Integer> localAggregation, Context context, boolean localAggregationCommand, Integer num) throws IOException, InterruptedException {
        if (localAggregationCommand) {
            localAggregation.put(key, localAggregation.getOrDefault(key, 0) + num);
        } else {
            word.set(key);
            count.set(num);
            context.write(word, count);
        }
    }
    
    private void flushLocalAggregation(Map<String, Integer> localAggregation, Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : localAggregation.entrySet()) {
            word.set(entry.getKey());
            count.set(entry.getValue());
            context.write(word, count);
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;

            try {
                for (IntWritable value : values) {
                    sum += value.get();
                }
                
                if (sum > Integer.MAX_VALUE) { // if key is long
                    context.setStatus("Warning: Sum overflow for key: " + key.toString());
                    sum = Integer.MAX_VALUE;
                }

                int numAsterisks = StringUtils.countMatches(key.toString(), Defs.astrix);
                int numWords = 3 - (numAsterisks % 3);
                
                if (numWords == 1) {  // w1**
                    sum = sum / 3;
                } else if (numWords == 2) {  // w1w2*
                    sum = sum / 2;
                }
                
                result.set((int)sum);
                context.write(key, result);
                
            } catch (Exception e) {
                // Log error and continue with next key
                context.setStatus("Error processing key: " + key.toString());
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            int numAsterisks = StringUtils.countMatches(key.toString(), Defs.astrix);
            int numWords = 3 - (numAsterisks % 3);
            // return numWords % numPartitions;
            return (numWords + key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calc variables step");
        job.setJarByClass(CalcVariablesStep.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant one
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(Defs.HEB_3Gram_path));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_word_count"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    }
}
