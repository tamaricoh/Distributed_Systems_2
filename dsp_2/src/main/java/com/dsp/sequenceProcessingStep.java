package com.dsp;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

// Custom writable for our composite value
public class sequenceProcessingStep {
    public static class WordSequenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
            // Input format: (w1 w2 w3)(count) or (w1 w2 *)(count) or (w1 * *)(count)
            String line = value.toString().trim();
            // Remove parentheses and split into parts
            String[] parts = line.substring(1, line.length() - 1).split("\\)\\(");
            String[] words = parts[0].split("\\s+");
            long count = Long.parseLong(parts[1]);
            
            // Original sequence
            outputKey.set(String.join(Defs.delimiter, words));
            outputValue.set("FULL:" + count);
            context.write(outputKey, outputValue);
            
            // Generate keys for sub-sequences based on the pattern
            if (!words[1].equals("*")) {
                // Output for w1-w2 pair
                outputKey.set(words[0] + " " + words[1] + " *");
                outputValue.set("PAIR12:" + count);
                context.write(outputKey, outputValue);
                
                if (!words[2].equals("*")) {
                    // Output for w2-w3 pair
                    outputKey.set("* " + words[1] + " " + words[2]);
                    outputValue.set("PAIR23:" + count);
                    context.write(outputKey, outputValue);
                    
                    // Individual words
                    outputKey.set("* " + words[1] + " *");
                    outputValue.set("SINGLE2:" + count);
                    context.write(outputKey, outputValue);
                    
                    outputKey.set("* * " + words[2]);
                    outputValue.set("SINGLE3:" + count);
                    context.write(outputKey, outputValue);
                }
            }
            
            // For C0 calculation
            if (words[1].equals("*") && words[2].equals("*")) {
                outputKey.set("TOTAL_C0");
                outputValue.set("C0:" + count);
                context.write(outputKey, outputValue);
            }
        }
    }
    
    public static class WordSequenceReducer 
            extends Reducer<Text, Text, Text, Text> {
        
        private long C0 = 0;
        
        @Override
        protected void setup(Context context) {
            C0 = 0;
        }
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            if (key.toString().equals("TOTAL_C0")) {
                // Calculate C0
                for (Text val : values) {
                    String[] parts = val.toString().split(":");
                    C0 += Long.parseLong(parts[1]);
                }
                return;
            }
            
            long fullCount = 0;
            long pair12Count = 0;
            long pair23Count = 0;
            long word2Count = 0;
            long word3Count = 0;
            
            // Process all values for this key
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String type = parts[0];
                long count = Long.parseLong(parts[1]);
                
                switch (type) {
                    case "FULL":
                        fullCount = count;
                        break;
                    case "PAIR12":
                        pair12Count = count;
                        break;
                    case "PAIR23":
                        pair23Count = count;
                        break;
                    case "SINGLE2":
                        word2Count = count;
                        break;
                    case "SINGLE3":
                        word3Count = count;
                        break;
                }
            }
            
            // Only output for full sequences (no asterisks)
            String[] keyParts = key.toString().split("\\s+");
            if (!keyParts[0].equals("*") && !keyParts[1].equals("*") && !keyParts[2].equals("*")) {
                String output = String.format("%d,%d,%d,%d,%d,%d", 
                    fullCount, pair12Count, pair23Count, word2Count, word3Count, C0);
                context.write(key, new Text(output));
            }
        }
    }
    
    public static class WordSequencePartitioner 
            extends Partitioner<Text, Text> {
        
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            // Special handling for C0 calculation
            if (key.toString().equals("TOTAL_C0")) {
                return 0;
            }
            // Use first word for partitioning to ensure related sequences go to same reducer
            String[] words = key.toString().split("\\s+");
            return Math.abs(words[0].hashCode() % numReduceTasks);
        }
    }
}
