package com.dsp;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;


public class AWS_Hadoop_setup { 
    static AWS aws = AWS.getInstance();
    
    public static void main(String[] args) {
        //aws.createBucketIfNotExists(Defs.PROJECT_NAME);
        aws.createSqsQueue(Defs.C0_SQS);
        aws.uploadFileToS3(Defs.stopWordsFile, Defs.PROJECT_NAME); // StopWords
        aws.uploadFileToS3(Defs.testingFiles[0], Defs.PROJECT_NAME); 
        for(int i =0; i<Defs.Steps_Names.length; i++){
            aws.uploadFileToS3(Defs.PATH_TO_TARGET + Defs.Steps_Names[i] + ".jar", Defs.PROJECT_NAME);
        } 

        EmrClient emrClient = EmrClient.builder()
            .region(Defs.region1)
            .build();
        
        List<HadoopJarStepConfig> hadoopJarSteps = new ArrayList<>();
        for(int i = 0; i < Defs.Steps_Names.length; i++) { 
            hadoopJarSteps.add(HadoopJarStepConfig.builder()
                .jar(Defs.getStepJarPath(i))        //path to jar in s3
                .mainClass(Defs.Steps_Names[i])
                .args(Defs.getStepArgs(i))
                .build());
            System.out.println("Created jar step config for " + Defs.Steps_Names[i]);
        }

        List<StepConfig> stepConfigs = new ArrayList<>();
        for(int i = 0; i < Defs.Steps_Names.length; i++) { 
            stepConfigs.add(StepConfig.builder()
                .name(Defs.Steps_Names[i])
                .hadoopJarStep(hadoopJarSteps.get(i))
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .build());
            System.out.println("Created step config for " + Defs.Steps_Names[i]);
        }

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
            .instanceCount(Defs.instanceCount)
            .masterInstanceType("m4.large")
            .slaveInstanceType("m4.large")
            .hadoopVersion(Defs.HADOOP_VER)
            .ec2KeyName(Defs.KEY_NAME_SSH)
            .keepJobFlowAliveWhenNoSteps(false)
            .placement(PlacementType.builder()
                .availabilityZone(Defs.placementRegion)
                .build())
            .build();
        System.out.println("Created instances config.");

        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name(Defs.PROJECT_NAME)
                .instances(instances)
                .steps(stepConfigs)
                .logUri(Defs.Logs_URI)
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
                .build();
        System.out.println("Created run flow request.");
        
        RunJobFlowResponse runJobFlowResult = emrClient.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.jobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}