package com.dsp;

// import java.io.BufferedReader;
// import java.io.FileReader;
import java.io.IOException;
// import java.util.ArrayList;
import java.util.Arrays;
// import java.util.List;
import org.apache.hadoop.io.Text;
// import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// import com.fasterxml.jackson.jaxrs.json.annotation.JSONP.Def;

import org.apache.hadoop.mapreduce.Partitioner;
// import org.apache.commons.lang.StringUtils;

// Custom writable for our composite value
public class valuesJoinerStep {

    // Mapper class for the second step of sequence processing
    public static class MapperClass extends Mapper<Text, IntWritable, Text, Text> {
        private Text newVal = new Text();
        private Text newKey = new Text();

        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            String keyStr = key.toString().trim();
            String[] keyWords = keyStr.split(Defs.delimiter);

            if (keyWords[1].equals(Defs.astrix)){ // w1**
                newKey = new Text(keyWords[0]);
                newVal = new Text("Single" + Defs.seperator + Integer.toString(value.get()));
                context.write(newKey, newVal);
            }
            else if(keyWords[2].equals(Defs.astrix)){ // w1w2*
                newKey = new Text(keyWords[0] + Defs.delimiter + keyWords[1]);
                newVal = new Text("Double" + Defs.seperator + Integer.toString(value.get()));
                context.write(newKey, newVal);
            }
            else if(keyStr.equals("c0")){
                
            }
            else {
                newKey = new Text(keyWords[1]); 
                newVal = new Text(keyStr + Defs.seperator + "C1:" + Defs.seperator + Defs.seperator + "N3:" + Defs.seperator + Integer.toString(value.get()));
                context.write(newKey, newVal); 
                
                newKey = new Text(keyWords[2]);
                newVal = new Text(keyStr + Defs.seperator + "N1:" + Defs.seperator + Defs.seperator + "N3:" + Defs.seperator + Integer.toString(value.get()));
                context.write(newKey, newVal);

                newKey = new Text(keyWords[0] + Defs.delimiter + keyWords[1]);
                newVal = new Text(keyStr + Defs.seperator + "C2:" + Defs.seperator + Defs.seperator + "N3:" + Defs.seperator + Integer.toString(value.get()));
                context.write(newKey, newVal);

                newKey = new Text(keyWords[1] + Defs.delimiter + keyWords[2]);
                newVal = new Text(keyStr + Defs.seperator + "N2:" + Defs.seperator + Defs.seperator + "N3:" + Defs.seperator + Integer.toString(value.get()));
                context.write(newKey, newVal);
            }
        }
    }

    // Reducer class for the second step of sequence processing
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private Text newVal = new Text();
        private Text newKey = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String count = "";

            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.startsWith("Single") || valueStr.startsWith("Double")) {
                    String[] parts = valueStr.split(Defs.seperator);
                    count = parts[parts.length - 1];
                    break;
                }
            }

            for (Text value : values) {
                String valueStr = value.toString();
                if (!valueStr.startsWith("Single") && !valueStr.startsWith("Double")) {
                    String[] parts = valueStr.split(Defs.seperator);
                    parts[2] = count;
                    newVal.set(String.join(Defs.seperator, Arrays.copyOfRange(parts, 1, parts.length)));
                    newKey.set(parts[0]);
                    context.write(newKey, newVal);
                }
            }

        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
}
