package com.dsp;

import java.nio.file.Path;
import java.nio.file.Paths;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.nio.file.Paths;

import software.amazon.awssdk.regions.Region;


public class AWS {
    private final SqsClient sqs;
    private final S3Client s3;
    public static Region region = Region.US_EAST_1;
    private static final AWS instance = new AWS();

    private AWS() {
        sqs = SqsClient.builder().region(region).build();
        s3 = S3Client.builder().region(region).build();
    }

    public static AWS getInstance() {
        return instance;
    } 

    public void createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }

    public void createBucketIfNotExists(String bucketName) {
        try {
            // Create the bucket with the specified configuration
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());

            // Wait until the bucket exists
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void sendSQSMessage(String queueName, String message) {
        try{
                SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                        .queueUrl(getQueueUrl(queueName))
                        .messageBody(message)
                        .build();
                        getInstance().sqs.sendMessage(sendMessageRequest);
        }catch (SqsException e){
                System.err.println("[DEBUG]: Error trying to send message to queue " + queueName + ", Error Message: " + e.awsErrorDetails().errorMessage());
        }
    }

    public String uploadFileToS3(String input_file_path, String bucketName) {
        String s3Key = Paths.get(input_file_path).getFileName().toString();
        if (s3Key.contentEquals("terminate") || s3Key.contentEquals("FileNoTFound")){
            return null;
        }

        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            Path path = Paths.get(input_file_path);
            getInstance().s3.putObject(putObjectRequest, path);
            System.out.println("File uploaded successfully to S3: " + s3Key);
            return s3Key;
        } catch (Exception e) {
            System.err.println("Unexpected error during file upload: " + e.getMessage());
            return null;
        }
    }

    public static String getQueueUrl(String QueueName) {
        // Get the URL of the SQS queue by name
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(QueueName)
                .build();
        GetQueueUrlResponse getQueueUrlResponse = getInstance().sqs.getQueueUrl(getQueueUrlRequest);
        return getQueueUrlResponse.queueUrl();
    }

    public int checkSQSQueue(String queueName) {
        try {
            // Retrieve the queue URL
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            GetQueueUrlResponse queueUrlResponse = getInstance().sqs.getQueueUrl(getQueueUrlRequest);
            String queueUrl = queueUrlResponse.queueUrl();

            // Receive up to 1 messages
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build();

            ReceiveMessageResponse receiveMessageResponse = getInstance().sqs.receiveMessage(receiveMessageRequest);

            // Loop through the received messages
            for (Message message : receiveMessageResponse.messages()) {
                String messageBody = message.body();
                int C0 = Integer.parseInt(messageBody);
                return C0;
            }
        } catch (SqsException e) {
            System.err.println("Error checking SQS queue: " + e.awsErrorDetails().errorMessage());
        }
        return -1; // Return this if no matching message is found
    }
}
