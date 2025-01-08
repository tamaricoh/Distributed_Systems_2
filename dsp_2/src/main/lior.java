package com.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.util.Comparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class lior 
{
	private static final String bucketName = "ass2-lior-bucket";

	public class Step4Mapper extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// Key: w1,w2,w3 | Value: P(w3|w1,w2)
			String keyStr = key.toString();
			String valueStr = value.toString();
	
			String[] words = keyStr.split(",");
			if (words.length == 3) {
				String w1w2 = words[0] + " " + words[1]; // Combine w1 and w2
				String w3 = words[2];                   // Extract w3
	
				// Emit <w1w2, w3:probability>
				context.write(new Text(w1w2), new Text(w3 + ":" + valueStr));
			}
		}
	}

public class Step4Reducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Key: w1w2 | Values: [w3:probability, ...]
        String w1w2 = key.toString();
        ArrayList<String> wordProbList = new ArrayList<>();

        for (Text value : values) {
            wordProbList.add(value.toString()); // Collect all w3:probability pairs
        }

		Collections.sort(wordProbList, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				double prob1 = Double.parseDouble(o1.split(":")[1]);
				double prob2 = Double.parseDouble(o2.split(":")[1]);
				return Double.compare(prob2, prob1); // Descending order
			}
		});

        // Emit sorted <w1,w2,w3> with probability
        for (String wordProb : wordProbList) {
            String[] parts = wordProb.split(":");
            String w3 = parts[0];
            String probability = parts[1];

            // Emit <w1 w2 w3, probability>
            context.write(new Text(w1w2 + " " + w3), new Text(probability));
        }
    }
}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("bucket_name",bucketName);
		Job job = Job.getInstance(conf, "step_4");
		job.setJarByClass(Step4_Lior.class);
		job.setMapperClass(Step4Mapper.class);
		job.setReducerClass(Step4Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
