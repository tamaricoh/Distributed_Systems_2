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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CalcVariablesStep 
{

	public static class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		private final Set<String> stopWords = new HashSet<>();
		private Text newKey = new Text();
		private DoubleWritable newVal = new DoubleWritable();
		private static final int MAX_MAP_SIZE = 1000;
        private static final boolean localAggregationCommand = Defs.localAggregationCommand;
        private static Map<String, Double> localAggregation = new HashMap<>();

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

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String parts[] = value.toString().split("\t");
			Double count = Double.parseDouble(parts[2]);
			String[] words = parts[0].split(" ");
 			if(!only_hebrew_words(words)){
				return;
			}
            if (words.length > 2) {
                String w1 = words[0];
                String w2 = words[1];
                String w3 = words[2];
                if (stopWords.contains(w1) || stopWords.contains(w2) || stopWords.contains(w3))
                    return;
                countOccurrence("C0", count , context);                       // for C0
                countOccurrence(w1, count , context);                             // for N1\C1
                countOccurrence(w2, count , context);                             // for N1\C1
                countOccurrence(w3, count , context);                             // for N1\C1
                countOccurrence(w1 + Defs.delimiter + w2, count , context);       // for N2\C2
                countOccurrence(w2 + Defs.delimiter + w3, count , context);       // for N2\C2
                countOccurrence(w1 + Defs.delimiter + w2 + Defs.delimiter + w3, count , context);// for N3\C3

                if (localAggregationCommand && localAggregation.size() >= MAX_MAP_SIZE) {
                    context.setStatus("[DEBUG] Flushing local aggregation due to memory limit.");
                    flushLocalAggregation(context);
                }
            }
			else 
				return;
		}

		private void countOccurrence(String key, Double val, Context context) throws IOException, InterruptedException {
			if (localAggregationCommand) {
				localAggregation.put(key, localAggregation.getOrDefault(key, 0.0) + val);
			} else {
				newKey.set(key);
				newVal.set(val);
				context.write(newKey, newVal);
			}
		}
                
		private void flushLocalAggregation(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String, Double> entry : localAggregation.entrySet()) {
				newKey.set(entry.getKey());
				newVal.set(entry.getValue());
				context.write(newKey, newVal);
			}
			localAggregation.clear();
		}

		private static Boolean only_hebrew_words(String[] trigram){
			Boolean is_hebrew = true;
			for(String word : trigram){
				is_hebrew = is_hebrew && word.matches("^[א-ת]+$");
			}
			return is_hebrew;
		}
	}
	public static class ReducerClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		// private static LongWritable count = new LongWritable();
		static AWS aws = AWS.getInstance();
		private Text newKey = new Text();
		private DoubleWritable newVal = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
            Double sum = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
			if(key.toString().equals("C0")){
				aws.sendSQSMessage(Defs.C0_SQS, String.valueOf(Math.round(sum / 3)));
			}
			else {
				Integer num_words = key.toString().split("\\$\\$").length;
				newKey.set(key.toString());
				newVal.set(Math.floor(sum / num_words));
				context.write(newKey, newVal);
			}
            
        }
    }
	
	public static class PartitionerClass extends Partitioner<Text, DoubleWritable> {
        @Override
        public int getPartition(Text key, DoubleWritable value, int numPartitions) {
            return (numPartitions == 0) ? 0 : Math.abs(key.hashCode() % numPartitions);
        }
    }

	public static class CombinerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable combinedValue = new DoubleWritable();
	
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double partialSum = 0.0;
	
			// Calculate the partial sum of values for the key
			for (DoubleWritable value : values) {
				partialSum += value.get();
			}
	
			// Emit the key with the partial sum
			combinedValue.set(partialSum);
			context.write(key, combinedValue);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("bucket_name", bucketName);
		Job job = Job.getInstance(conf, args[0]);
		job.setJarByClass(CalcVariablesStep.class);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
		if(Defs.localAggregationCommand){
			job.setCombinerClass(CombinerClass.class);
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class); //FOR HEB-3GRAMS
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
