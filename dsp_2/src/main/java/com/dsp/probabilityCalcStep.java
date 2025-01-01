package com.dsp;

// import java.io.BufferedReader;
// import java.io.FileReader;
import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
import org.apache.hadoop.io.Text;
// import org.apache.commons.lang3.StringUtils;
// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// import com.fasterxml.jackson.jaxrs.json.annotation.JSONP.Def;

import org.apache.hadoop.mapreduce.Partitioner;


// Custom writable for our composite value
public class probabilityCalcStep {

    // Mapper class for the second step of sequence processing
    public static class MapperClass extends Mapper<Text, Text, Text, Text> {
        // private Text newVal = new Text();
        // private Text newKey = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    // Reducer class for the second step of sequence processing
    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {

        private DoubleWritable newVal = new DoubleWritable();
        static AWS aws = AWS.getInstance();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] nums = new int[5];

            for (Text value : values){
                String valueStr = value.toString();
                String[] parts = valueStr.split(Defs.seperator);
                nums[2] = Integer.parseInt(parts[parts.length - 1]);
                switch (parts[1]) {
                    case "N1:":
                        nums[0] = Integer.parseInt(parts[1]);

                    case "N2:":
                        nums[1] = Integer.parseInt(parts[1]);
                    
                    case "C1:":
                        nums[3] = Integer.parseInt(parts[1]);
                    
                    case "C2:":
                        nums[4] = Integer.parseInt(parts[1]);
                
                    default:
                        context.setStatus("Error processing num values: " + valueStr +" of key "+ key.toString());
                        continue;
                }
            }
            int C0 = aws.checkSQSQueue(Defs.C0_SQS);
            double p = Calc.calcP(nums[0], nums[1], nums[2], C0, nums[3], nums[4]);
            newVal.set(p);
            context.write(key, newVal);
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
}
