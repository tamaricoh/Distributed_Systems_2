package com.dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class sortSequencesStep {
    // Custom composite key class to hold w1, w2, w3
    public static class CompositeKey implements WritableComparable<CompositeKey> {
        private String w1;
        private String w2;
        private String w3;
        private double value;

        public CompositeKey() {}

        public CompositeKey(String w1, String w2, String w3, double value) {
            this.w1 = w1;
            this.w2 = w2;
            this.w3 = w3;
            this.value = value;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, w1);
            Text.writeString(out, w2);
            Text.writeString(out, w3);
            out.writeDouble(value);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            w1 = Text.readString(in);
            w2 = Text.readString(in);
            w3 = Text.readString(in);
            value = in.readDouble();
        }

        @Override
        public int compareTo(CompositeKey other) {
            // First compare w1
            int w1Compare = this.w1.compareTo(other.w1);
            if (w1Compare != 0) {
                return w1Compare;
            }
            
            // Then compare w2 (descending)
            int w2Compare = other.w2.compareTo(this.w2);  // Note the reversed order
            if (w2Compare != 0) {
                return w2Compare;
            }
            
            // Finally compare value (ascending)
            return Double.compare(this.value, other.value);
        }

        // Getters
        public String getW1() { return w1; }
        public String getW2() { return w2; }
        public String getW3() { return w3; }
        public double getValue() { return value; }
    }

    public static class CustomMapper 
            extends Mapper<Text, DoubleWritable, CompositeKey, DoubleWritable> {
        
        @Override
        protected void map(Text key, DoubleWritable value, Context context) 
                throws IOException, InterruptedException {
            System.out.println("Mapper Input - Key: " + key.toString() + ", Value: " + value.get());
            // Split the key into w1, w2, w3
            String[] parts = key.toString().split(Defs.delimiter);
            if (parts.length >= 3) {
                CompositeKey compositeKey = new CompositeKey(parts[0], parts[1], parts[2], value.get());
                context.write(compositeKey, value);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static class CustomReducer 
            extends Reducer<CompositeKey, DoubleWritable, Text, DoubleWritable> {
        
        @Override
        protected void reduce(CompositeKey key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            // Reconstruct the original key format
            String originalKey = key.getW1() + " " + key.getW2() + " " + key.getW3();
            
            // Write each value (there should be only one, but we'll iterate just in case)
            for (DoubleWritable value : values) {
                context.write(new Text(originalKey), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[0]);
        job.setJarByClass(sortSequencesStep.class);

        // Set input/output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Set input/output formats
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set mapper and reducer
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);
        job.setPartitionerClass(PartitionerClass.class);

        // Set map output key/value classes
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set output key/value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
