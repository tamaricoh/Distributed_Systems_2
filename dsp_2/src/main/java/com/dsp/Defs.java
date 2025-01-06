package com.dsp;

import software.amazon.awssdk.regions.Region;

public class Defs {
    public static String stopWordsFile = "/home/yarden/Distributed_Systems_2/dsp_2/src/main/resources/heb-stopwords.txt";
    public static String logsFile = "/home/yarden/Distributed_Systems_2/dsp_2/src/main/resources/logs.txt";
    public static String jarPath = "/home/yarden/Distributed_Systems_2/dsp_2/target/";
    public static String[] testingFiles = {"/home/yarden/Distributed_Systems_2/dsp_2/src/main/resources/hebrew-3grams.txt"};
    public static boolean localAggregationCommand = true;

    public static final String delimiter = "$$";
    public static final String seperator = "%%";
    public static final String astrix = "*";
    public static final String TAB = "\t";
    public static final String SPACE = " ";

    public enum ValueType {}

    public static Region region1 = Region.US_EAST_1;
    public static Region region2 = Region.US_WEST_2;
    public static String placementRegion = "us-east-1a"; 

    public static final int instanceCount = 2;

    public static final String HADOOP_VER = "3.3.6";
    public static final String KEY_NAME_SSH = "vockey";

    public static final String TERMINATE_JOB_FLOW_MESSAGE = "TERMINATE_JOB_FLOW";

    public static final String PROJECT_NAME = "word-prediction";
    public static final String Logs_URI = "s3://" + PROJECT_NAME + "/logs";
    public static final String C0_SQS = "C0-sqs";

    public static final String HEB_3Gram_path = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    public static final String S3_BUCKET_PATH = "s3://" + PROJECT_NAME + "/";
    public static final String PATH_TO_TARGET = "/home/yarden/Distributed_Systems_2/dsp_2/target/";
    public static final String[] Steps_Names = {"CalcVariablesStep", "valuesJoinerStep", "probabilityCalcStep", "trigramListStep"};
    public static final String[] Step_Output_Name = {"word-sequences", "triple-sequences", "word-sequence-probabillity", "trigram-result-file"};

    public static String getStepJarPath(int i){
        return  getPathS3(Steps_Names[i], ".jar");
    }
    public static String getPathS3(String file_name, String file_format){
        return S3_BUCKET_PATH + file_name + file_format;
    }
    public static String[] getStepArgs(int stepNum){
        String[] args;
        switch (stepNum){
            case 0:
                args = new String[]{ 
                    //HEB_3Gram_path,
                    "s3://" + PROJECT_NAME + "/" + "hebrew-3grams.txt",
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0] 
                };
                System.out.println("[DEBUG]  step output name: " + Step_Output_Name[0]);
                break;
            case 1:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0] + "/" , // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1] // output
                };
                break;
            case 2:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1] + "/", // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[2] // output
                };
                break;
            case 3:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1] + "/", // input
                    S3_BUCKET_PATH + Step_Output_Name[3] // output
                };
                break;
            default:
                args = new String[]{};
        }
        return args;
    }
}
