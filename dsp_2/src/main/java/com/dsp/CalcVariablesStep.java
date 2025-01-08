package com.dsp;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.lang3.StringUtils;

public class CalcVariablesStep 
{

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>{
		private final Set<String> stopWords = new HashSet<>();
		private Text newKey = new Text();
		private Text newVal = new Text();
		private int num;
        private final static IntWritable count = new IntWritable();
		private static final int MAX_MAP_SIZE = 1000;
        private static final boolean localAggregationCommand = Defs.localAggregationCommand;
        private static Map<String, Integer> localAggregation = new HashMap<>();

		protected void setup(Context context) throws IOException {
			String stop_words = AWS.getInstance().downloadFromS3(Defs.PROJECT_NAME, "heb-stopwords.txt", "/tmp");
			if (stop_words == null) {
				throw new IOException("Failed to download stop words file from S3");
			}
			try (BufferedReader reader = new BufferedReader(new FileReader(stop_words))) {
				String line;
				while ((line = reader.readLine()) != null) {
					stopWords.add(line.trim());
				}
			} catch (IOException e) {
				throw new IOException("Error reading stop words file: " + e.getMessage());
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String parts[] = value.toString().split("\t");
			String[] words = parts[0].split(" ");

            if (words.length > 2 && parts.length > 2) {
                String w1 = words[0];
                String w2 = words[1];
                String w3 = words[2];
                if (stopWords.contains(w1) || stopWords.contains(w2) || stopWords.contains(w3))
                    return;
                                    
                countOccurrence("C0", parts[2] , context);                                      // for C0
                countOccurrence(w1, parts[2] , context);                                            // for N1\C1
                countOccurrence(w2, parts[2] , context);                                            // for N1\C1
                countOccurrence(w3, parts[2] , context);                                            // for N1\C1
                countOccurrence(w1 + Defs.delimiter + w2, parts[2] , context);                      // for N2\C2
                countOccurrence(w2 + Defs.delimiter + w3, parts[2] , context);                      // for N2\C2
                countOccurrence(w1 + Defs.delimiter + w2 + Defs.delimiter + w3, parts[2] , context);// for N3\C3

                if (localAggregationCommand && localAggregation.size() >= MAX_MAP_SIZE) {
                    context.setStatus("[DEBUG] Flushing local aggregation due to memory limit.");
                    flushLocalAggregation(context);
                    localAggregation.clear();
                }
            }
			else 
				return;
	}

    private void countOccurrence(String key, String val, Context context) throws IOException, InterruptedException {
        if (localAggregationCommand) {
            localAggregation.put(key, localAggregation.getOrDefault(key, 0) + Integer.parseInt(val));
        } else {
            newKey.set(key);
            newVal.set(val);
            context.write(newKey, newVal);
        }
    }
                
    private void flushLocalAggregation(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : localAggregation.entrySet()) {
            newKey.set(entry.getKey());
            newVal.set(String.valueOf(entry.getValue()));
            context.write(newKey, newVal);
        }
    }
}
	public static class ReducerClass extends Reducer<Text,Text,Text,Text>
	{
		// private static LongWritable count = new LongWritable();
		static AWS aws = AWS.getInstance();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
            Double sum = 0.0;
            for (Text value : values) {
                sum += Double.parseDouble(value.toString());
            }
			if(key.toString().equals("C0" + Defs.astrix + Defs.astrix)){
				aws.createSqsQueue(Defs.C0_SQS);
				aws.sendSQSMessage(Defs.C0_SQS, String.valueOf(sum));
			}
			else {
				context.write(key, new Text(Double.toString(sum)));
			}
            
        }
    }
	

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		// conf.set("bucket_name", bucketName);
		Job job = Job.getInstance(conf, args[0]);
		job.setJarByClass(CalcVariablesStep.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
        //maybe need to add partinior for the actual 3grams input
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);/////SequenceFileInputFormat.class); //FOR HEB-3GRAMS
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
