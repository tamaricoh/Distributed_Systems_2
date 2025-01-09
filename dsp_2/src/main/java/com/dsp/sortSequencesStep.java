package com.dsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
        private Double value;

        public CompositeKey() {}

        public CompositeKey(String w1, String w2, String w3, Double value) {
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
            int w2Compare = other.w2.compareTo(this.w2);
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

    public static class MapperClass extends Mapper<Object, Text, CompositeKey, Text> {
        private CompositeKey newKey = new CompositeKey();
        private Text newVal = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //  input format: "w1 w2 w3\tvalue"
        String[] parts = value.toString().split("\t");
        String[] trigram = parts[0].split(" ");
        newKey = new CompositeKey(trigram[0], trigram[1],trigram[2], Double.parseDouble(parts[1]));
        newVal.set(trigram[2]);

        context.write(newKey, newVal);
    }
    }

    public static class PartitionerClass extends Partitioner<CompositeKey, Text> {
        @Override
        public int getPartition(CompositeKey key, Text value, int numPartitions) {
            return (key.getW1().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<CompositeKey, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newVal = new Text();
    
        @Override
        protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Process the sorted input
            for (Text value : values) {
                // Output format: "w1 w2 w3" -> "value"
                newKey.set(key.getW1() + " " + key.getW2() + " " + value.toString());
                newVal.set(String.valueOf(key.getValue()));
                context.write(newKey, newVal);
            }
        }
    }


    
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[0]);
        job.setJarByClass(sortSequencesStep.class);
        // Set map, reduce, partitinior
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setNumReduceTasks(1); //Ensuring all key-val goes to the same reducer

        // Set input/output paths
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // Set map output key/value classes
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(Text.class);

        // Set output key/value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        boolean job_finished = job.waitForCompletion(true);
        if(job_finished){
            AWS aws = AWS.getInstance();
            aws.createSqsQueue("job-completion-time");
            aws.sendSQSMessage("job-completion-time", "the process: " + job.getJobID().getJtIdentifier() 
                                                                + "ran for: " + String.valueOf(job.getFinishTime()-job.getStartTime()));
            
        
        }
    }
}
