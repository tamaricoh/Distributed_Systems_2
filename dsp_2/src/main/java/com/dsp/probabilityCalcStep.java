package com.dsp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class probabilityCalcStep {

    // Mapper class for the second step of probability 
    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    // Reducer class for the second step of sequence processing
    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {

        private Text newKey = new Text();
        private DoubleWritable newVal = new DoubleWritable();
        static AWS aws = AWS.getInstance();
        private static Double C0;
        
        @Override
        protected void setup(Context context) throws IOException {
			C0 = aws.checkSQSQueue(Defs.C0_SQS);
		}


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double[] nums = new Double[]{0.0, 0.0, 0.0, 1.0, 1.0};
            for (Text value : values){
                // value = C2:%%465.0%%N3:%%465.0
                String valueStr = value.toString();
                String[] parts = value.toString().split("\\%\\%");
                nums[2] = Double.parseDouble(parts[parts.length - 1]);
                switch (parts[0]) {
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
            Double p = calcP(nums[0], nums[1], nums[2], C0, nums[3], nums[4]);
            newKey.set(key.toString().replace(Defs.delimiter, Defs.SPACE));
            newVal.set(p);
            context.write(newKey, newVal);
        }

        private static Double calcP(Double n1, Double n2, Double n3, Double c0, Double c1, Double c2) {
            double k2 = calcKi(n2);
            double k3 = calcKi(n3);
            double first = k3*(n3/c2);
            double second = (1-k3)*k2*(n2/c1);
            double third = (1-k3)*(1-k2)*(n1/c0);
            return first + second + third;
        }
    
        
        private static double calcKi(Double ni) {
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, args[0]);
        job.setJarByClass(probabilityCalcStep.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(TextFileOutputFormat.class);
        //job.setInputFormatClass(TextFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
