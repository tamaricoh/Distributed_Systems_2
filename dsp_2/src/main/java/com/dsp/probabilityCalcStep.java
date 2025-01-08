package com.dsp;

// import java.io.BufferedReader;
// import java.io.FileReader;
import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
// import org.apache.commons.lang3.StringUtils;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

// import com.fasterxml.jackson.jaxrs.json.annotation.JSONP.Def;

import org.apache.hadoop.mapreduce.Partitioner;


// Custom writable for our composite value
public class probabilityCalcStep {

    // Mapper class for the second step of sequence processing
    public static class MapperClass extends Mapper<Object, Text, Text, Text> {
        private Text newVal = new Text();
        private Text newKey = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            newKey.set(parts[0]);
            newVal.set(parts[1]);
            context.write(newKey, newVal);
        }
    }

    // Reducer class for the second step of sequence processing
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private Text newKey = new Text();
        private Text newVal = new Text();
        static AWS aws = AWS.getInstance();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[] nums = new double[5];
            

            for (Text value : values){
                // value = 
                String valueStr = value.toString();
                String[] parts = value.toString().split("\\&\\&");
                nums[2] = Double.parseDouble(parts[parts.length - 1]);
                switch (parts[1]) {
                    case "N1:":
                        nums[0] = Double.parseDouble(parts[1]);
                        break;
                    case "N2:":
                        nums[1] = Double.parseDouble(parts[1]);
                        break;
                    case "C1:":
                        nums[3] = Double.parseDouble(parts[1]);
                        break;
                    case "C2:":
                        nums[4] = Double.parseDouble(parts[1]);
                        break;
                    default:
                        context.setStatus("Error processing num values: " + valueStr +" of key "+ key.toString());
                        continue;
                }
            }
            int C0 = aws.checkSQSQueue(Defs.C0_SQS);
            double p = calcP((int) nums[0],(int) nums[1],(int) nums[2],(int) C0,(int) nums[3],(int) nums[4]);
            newVal.set(String.valueOf(p));
            context.write(key, newVal);
        }

        private static double calcP(int n1, int n2, int n3, int c0, int c1, int c2) {
            double k2 = calcKi(n2);
            double k3 = calcKi(n3);
            double first = k3*(n3/c2);
            double second = (1-k3)*k2*(n2/c1);
            double third = (1-k3)*(1-k2)*(n1/c0);
            return first + second + third;
        }
    
        
        private static double calcKi(int ni) {
            double log_ni = Math.log(ni+1);
            double numerator = log_ni + 1;
            double denominator = log_ni + 2;
            return numerator/denominator;
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (numPartitions == 0) ? 0 : Math.abs(key.hashCode() % numPartitions);
        }
    }

        public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[0]);
        job.setJarByClass(probabilityCalcStep.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
