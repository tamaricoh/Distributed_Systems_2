package com.dsp;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class AWS_Hadoop_setup { 
    
    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            System.out.println("ERROR! Invalid input usage: java CollocationExtraction <minPmi> <relMinPmi>");
            System.exit(1);
        }

        // Naming_conventions.minNpmi = args[0];
        // Naming_conventions.relMinNpmi = args[1];

        //BasicConfigurator.configure(); // Add a ConsoleAppender that uses PatternLayout using the PatternLayout.TTCC_CONVERSION_PATTERN and prints to System.out to the root category.
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withRegion(Naming_conventions.regions).build();
        //System.out.println(mapReduce.listClusters());

        List<HadoopJarStepConfig> hadoopJarStepConfigs = new ArrayList<HadoopJarStepConfig>();
        for(int i = 0 ; i < Naming_conventions.Steps_Names.length ; i++) { 
            hadoopJarStepConfigs.add(new HadoopJarStepConfig()
                    .withJar(Naming_conventions.JAR_PATH) 
                    .withMainClass(Naming_conventions.Steps_Names[i])
                    .withArgs(Naming_conventions.getStepArgs(i)));
            System.out.println("Created jar step config for " + Naming_conventions.Steps_Names[i]);
        }

        List<StepConfig> stepConfigs = new ArrayList<StepConfig>();
        for(int i = 0 ; i < Naming_conventions.Steps_Names.length ; i++) { 
            stepConfigs.add(new StepConfig()
                    .withName(Naming_conventions.Steps_Names[i]) 
                    .withHadoopJarStep(hadoopJarStepConfigs.get(i))
                    .withActionOnFailure(Naming_conventions.TERMINATE_JOB_FLOW_MESSAGE));
            System.out.println("Created step config for " + Naming_conventions.Steps_Names[i]);
        }

    
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(Naming_conventions.instanceCount)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion(Naming_conventions.HADOOP_VER)
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(Naming_conventions.placementRegion));
        System.out.println("Created instances config.");

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName(Naming_conventions.PROJECT_NAME)
                .withInstances(instances)
                .withSteps(stepConfigs)
                .withLogUri(Naming_conventions.Logs_URI)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");;
        System.out.println("Created run flow request.");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
    
}
