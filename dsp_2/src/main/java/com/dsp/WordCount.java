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

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;


public class WordCount {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            boolean localAggregationCommand = Naming_conventions.localAggregationCommand;
            StringTokenizer itr = new StringTokenizer(value.toString());
    
            String currentWord = null;
            String nextWord = null;
            String secondNextWord = null;
    

            Map<String, Integer> localAggregation = new HashMap<>();
    
            while (itr.hasMoreTokens()) {
                if (currentWord != null) {
                    nextWord = itr.nextToken();
                }
                if (nextWord != null) {
                    secondNextWord = itr.nextToken();
                }

                if(localAggregationCommand)
                    localAggregation.put(currentWord, localAggregation.getOrDefault(currentWord, 0) + 1);
                else {
                        word.set(currentWord);
                        context.write(word, one);
                }
    
                if (nextWord != null) {
                    String wordPair = currentWord + " " + nextWord;
                    if(localAggregationCommand)
                        localAggregation.put(wordPair, localAggregation.getOrDefault(wordPair, 0) + 1);
                    else {
                        word.set(wordPair);
                        context.write(word, one);
                    }
                }
    
                if (secondNextWord != null) {
                    String wordTriple = currentWord + " " + nextWord + " " + secondNextWord;
                    if(localAggregationCommand)
                        localAggregation.put(wordTriple, localAggregation.getOrDefault(wordTriple, 0) + 1);
                        else {
                            word.set(wordTriple);
                            context.write(word, one);
                        }
                }
    
                currentWord = nextWord;
                nextWord = secondNextWord;
                secondNextWord = null;
            }
    
            if (localAggregationCommand){
                for (Map.Entry<String, Integer> entry : localAggregation.entrySet()) {
                    word.set(entry.getKey());
                    context.write(word, new IntWritable(entry.getValue()));
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
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
