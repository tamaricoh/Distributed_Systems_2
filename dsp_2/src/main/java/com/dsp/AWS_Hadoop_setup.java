package com.dsp;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;


public class AWS_Hadoop_setup { 
    
    public static void main(String[] args) {
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withRegion(Defs.regions).build();
        
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
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));;
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
