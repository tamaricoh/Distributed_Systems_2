package com.dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class ProbabilityCalculator {
    
    // Custom writable for probability output
    public static class ProbabilityWritable implements WritableComparable<ProbabilityWritable> {
        private String w3;
        private double probability;
        
        public void set(String w3, double probability) {
            this.w3 = w3;
            this.probability = probability;
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, w3);
            out.writeDouble(probability);
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            w3 = Text.readString(in);
            probability = in.readDouble();
        }
        
        @Override
        public int compareTo(ProbabilityWritable o) {
            // Sort by probability in descending order
            return Double.compare(o.probability, this.probability);
        }
    }
    
    public static class ProbabilityMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, Long> counts;
        private long totalWords; // C0
        
        @Override
        protected void setup(Context context) throws IOException {
            counts = new HashMap<>();
            loadCounts(context);
        }
        
        private void loadCounts(Context context) throws IOException {
            // Load counts from the first job's output using distributed cache
            // This would be configured in the main method using:
            // job.addCacheFile(new URI("path_to_counts_file"));
            
            // For now, we'll assume the counts are in the input
            // In practice, you'd want to load this from a distributed cache file
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (!parts[0].startsWith("N3:")) return; // We only process N3 counts here
            
            // Extract w1,w2,w3 from the N3 count key
            String[] words = parts[0].substring(3).split(",");
            String w1 = words[0];
            String w2 = words[1];
            String w3 = words[2];
            
            double probability = calculateProbability(w1, w2, w3);
            
            // Emit with w1,w2 as key for sorting
            context.write(new Text(w1 + "," + w2), new Text(w3 + "\t" + probability));
        }
        
        private double calculateProbability(String w1, String w2, String w3) {
            // Get all required counts
            long n1 = getCounts("N1:" + w3);
            long n2 = getCounts("N2:" + w2 + "," + w3);
            long n3 = getCounts("N3:" + w1 + "," + w2 + "," + w3);
            long c0 = totalWords;
            long c1 = getCounts("C1:" + w2);
            long c2 = getCounts("C2:" + w1 + "," + w2);
            
            // Avoid division by zero
            if (c0 == 0 || c1 == 0 || c2 == 0) {
                return 0.0;
            }
            
            // Calculate λ1, λ2, λ3 according to Thede & Harper formula
            double lambda1 = (double) n3 / c2;
            double lambda2 = (double) n2 / c1;
            double lambda3 = (double) n1 / c0;
            
            // The final probability is the sum of the lambdas
            return lambda1 + lambda2 + lambda3;
        }
        
        private long getCounts(String key) {
            return counts.getOrDefault(key, 0L);
        }
    }
    
    public static class ProbabilityReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            // Sort probabilities for each w1,w2 pair
            List<Pair<String, Double>> probabilities = new ArrayList<>();
            
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                String w3 = parts[0];
                double prob = Double.parseDouble(parts[1]);
                probabilities.add(new Pair<>(w3, prob));
            }
            
            // Sort by probability in descending order
            probabilities.sort((a, b) -> Double.compare(b.second, a.second));
            
            // Output format: w1 w2    w3 probability
            StringBuilder output = new StringBuilder();
            for (Pair<String, Double> pair : probabilities) {
                if (output.length() > 0) {
                    output.append("\n");
                }
                output.append(String.format("%s %.6f", pair.first, pair.second));
            }
            
            context.write(key, new Text(output.toString()));
        }
    }
    
    // Helper class for sorting
    private static class Pair<A, B> {
        A first;
        B second;
        
        Pair(A first, B second) {
            this.first = first;
            this.second = second;
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Probability Calculation");
        
        job.setJarByClass(ProbabilityCalculator.class);
        job.setMapperClass(ProbabilityMapper.class);
        job.setReducerClass(ProbabilityReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Input should be the output from the counting job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}