package com.dsp;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.services.elasticmapreduce.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.elasticmapreduce.model.JobFlowInstancesConfig;


import software.amazon.awssdk.services.ec2.model.IamInstanceProfile;


public class AWS_Hadoop_setup { 
    static AWS aws = AWS.getInstance();
    
    public static void main(String[] args) {

        aws.createBucketIfNotExists(Defs.PROJECT_NAME);
        aws.uploadFileToS3(Defs.stopWordsFile, Defs.PROJECT_NAME); // StopWords
        aws.uploadFileToS3(Defs.testingFiles[0], Defs.PROJECT_NAME); 
        aws.uploadFileToS3("/home/yarden/Distributed_Systems_2/dsp_2/target/aws-hadoop-setup.jar", Defs.PROJECT_NAME); 
        /***for(int i = 0 ; i < Defs.Steps_Names.length ; i++){
            aws.uploadFileToS3(Defs.jarPath + Defs.Steps_Names[i] + ".jar", Defs.PROJECT_NAME); //Upload Jars
        }***/

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withRegion(Defs.region1).build();
        
        List<HadoopJarStepConfig> hadoopJarStepConfigs = new ArrayList<HadoopJarStepConfig>();
        for(int i = 0 ; i < Defs.Steps_Names.length ; i++) { 
            hadoopJarStepConfigs.add(new HadoopJarStepConfig()
                    .withJar(Defs.JAR_PATH) 
                    .withMainClass(Defs.Steps_Names[i])
                    .withArgs(Defs.getStepArgs(i)));
            System.out.println("Created jar step config for " + Defs.Steps_Names[i]);
        }

        List<StepConfig> stepConfigs = new ArrayList<StepConfig>();
        for(int i = 0 ; i < Defs.Steps_Names.length ; i++) { 
            stepConfigs.add(new StepConfig()
                    .withName(Defs.Steps_Names[i]) 
                    .withHadoopJarStep(hadoopJarStepConfigs.get(i))
                    .withActionOnFailure(Defs.TERMINATE_JOB_FLOW_MESSAGE));
            System.out.println("Created step config for " + Defs.Steps_Names[i]);
        }

    
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(Defs.instanceCount)
                .withMasterInstanceType("m4.large")
                .withSlaveInstanceType("m4.large")
                .withHadoopVersion(Defs.HADOOP_VER)
                .withEc2KeyName(Defs.KEY_NAME_SSH)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(Defs.placementRegion))
                .withIamInstanceProfile(iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build()));
        System.out.println("Created instances config.");

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName(Defs.PROJECT_NAME)
                .withInstances(instances)
                .withSteps(stepConfigs)
                .withLogUri(Defs.Logs_URI)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");;
        System.out.println("Created run flow request.");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
    
}
