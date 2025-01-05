package com.dsp;

import com.amazonaws.regions.Regions;

public class Defs {
    public static String stopWordsFile = "../resources/heb-stopwords.txt";

    public static boolean localAggregationCommand = true;

    public static final String delimiter = "$$";
    public static final String seperator = "%%";
    public static final String astrix = "*";
    public static final String TAB = "\t";
    public static final String SPACE = " "; // '\0'

    public enum ValueType {}

    public static Regions regions = Regions.US_EAST_1;
    public static String placementRegion = "us-east-1a"; 

    public static final int instanceCount = 9;

    public static final String HADOOP_VER = "3.3.6";
    public static final String KEY_NAME_SSH = "keyPair";

    public static final String TERMINATE_JOB_FLOW_MESSAGE = "TERMINATE_JOB_FLOW";

    public static final String PROJECT_NAME = "WordPrediction";
    public static final String JAR_NAME = "WordPredictionJar";
    public static final String JAR_PATH = "s3://" + PROJECT_NAME + "/" + JAR_NAME + ".jar";
    public static final String Logs_URI = "s3://" + PROJECT_NAME + "/logs";
    public static final String C0_SQS = "C0-sqs";

    public static final String HEB_3Gram_path = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";

    public static final String[] Steps_Names = {"CalcVariablesStep", "valuesJoinerStep", "probabilityCalcStep"};
    public static final String[] Step_Output_Name = {};

    public static String getStepJarPath(int i){
        return "s3://" + PROJECT_NAME + "/" + Steps_Names[i] + ".jar";
    }
    public static String[] getStepArgs(int stepNum){
        String[] args;
        switch (stepNum){
            case 0:
                args = new String[]{ 
                    HEB_3Gram_path,
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0] 
                };
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
            default:
                args = new String[]{};
        }
        return args;
    }
}
