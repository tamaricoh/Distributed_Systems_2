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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class WordCount {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private final String delimiter = Naming_conventions.delimiter;
        private static int c0 = 0;
        private final static IntWritable count = new IntWritable(1);
        private final Set<String> stopWords = new HashSet<>();
        private static final int MAX_MAP_SIZE = 1000;


    protected void setup(Context context) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader("../heb-stopwords.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line.trim());
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        boolean localAggregationCommand = Naming_conventions.localAggregationCommand;
        StringTokenizer itr = new StringTokenizer(value.toString());
        
        StringBuilder ngramBuilder = new StringBuilder();
        Map<String, Integer> localAggregation = new HashMap<>();
        
        String[] window = new String[3];
        int windowIndex = 0;
        
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            if (stopWords.stream().anyMatch(stopWord -> token.equals(stopWord))){ // || token.endsWith(stopWord))) {
                continue;
            }
            
            window[windowIndex % 3] = token;
            windowIndex++;
            
            if (windowIndex >= 1) {
                processNgram(window[(windowIndex - 1) % 3], null, null, 
                            localAggregation, context, localAggregationCommand,
                            ngramBuilder, word, count);
            }
            
            if (windowIndex >= 2) {
                processNgram(window[(windowIndex - 2) % 3], 
                            window[(windowIndex - 1) % 3], null,
                            localAggregation, context, localAggregationCommand,
                            ngramBuilder, word, count);
            }
            
            if (windowIndex >= 3) {
                processNgram(window[(windowIndex - 3) % 3],
                            window[(windowIndex - 2) % 3],
                            window[(windowIndex - 1) % 3],
                            localAggregation, context, localAggregationCommand,
                            ngramBuilder, word, count);
            }
            
            if (localAggregationCommand && localAggregation.size() >= MAX_MAP_SIZE) {
                flushLocalAggregation(localAggregation, context, word, count);
                localAggregation.clear();
            }
        }
        
        if (localAggregationCommand && !localAggregation.isEmpty()) {
            flushLocalAggregation(localAggregation, context, word, count);
        }
        
        word.set("c0");
        count.set(c0);
        context.write(word, count);
    }
    
    private void processNgram(String w1, String w2, String w3, Map<String, Integer> localAggregation, Context context, boolean localAggregationCommand, StringBuilder builder, Text word, IntWritable one) throws IOException, InterruptedException {
        builder.setLength(0);
        builder.append(w1);
        if (w2 != null) {
            builder.append(delimiter).append(w2);
        }
        if (w3 != null) {
            builder.append(delimiter).append(w3);
        }
        
        String ngram = builder.toString();
        if (localAggregationCommand) {
            localAggregation.put(ngram, localAggregation.getOrDefault(ngram, 0) + 1);
        } else {
            word.set(ngram);
            one.set(1);
            context.write(word, one);
        }
        c0++;
    }

    private void flushLocalAggregation(Map<String, Integer> localAggregation, Context context, Text word, IntWritable count) throws IOException, InterruptedException {
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
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path("s3://bucket163897429777/arbix.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_word_count"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    }
}
