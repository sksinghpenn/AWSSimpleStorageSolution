package com.lifotech.aws.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Manager {
	
	private static AWSCredentialsProvider getAWSCredentials() {
		AWSCredentialsProvider awsCredentialsProvider = new ClasspathPropertiesFileCredentialsProvider();		
		return awsCredentialsProvider;
	}
	
	public static void createBucket(String bucketName) {		
		AmazonS3Client amazonS3Client = new AmazonS3Client(getAWSCredentials());		
		amazonS3Client.createBucket(bucketName);		
	}
	
	public static List<Bucket> getBucketList() {
		// get  AWSCredentialsProvider
		AWSCredentialsProvider awsCredentialsProvider = getAWSCredentials();
		
		// getAmazonS3Client
		AmazonS3Client amazonS3Client = new AmazonS3Client(awsCredentialsProvider);
		
		return amazonS3Client.listBuckets();
	}
	
	public static void putObjectIntoBucket(String bucketName, String key, File file) {
		AWSCredentialsProvider awsCredentialsProvider = getAWSCredentials();
		AmazonS3Client amazonS3Client = new AmazonS3Client(awsCredentialsProvider);		
		amazonS3Client.putObject(new PutObjectRequest(bucketName, key, file));		
	}
	
	public static void downLoadObjectFromBucket(String bucketName, String key, String location) throws IOException{
		AWSCredentialsProvider awsCredentialsProvider = getAWSCredentials();
		AmazonS3Client amazonS3Client = new AmazonS3Client(awsCredentialsProvider);	
		S3Object s3Object = amazonS3Client.getObject(bucketName, key);
		
	
		S3ObjectInputStream objectInputStream = s3Object.getObjectContent();		
		File file = new File(location + key);		
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		
		int readByte = 0;
		
		while( (readByte = objectInputStream.read()) != -1) {
			fileOutputStream.write(readByte);
		}
		
		fileOutputStream.close();
	}
	
	
	public static List<S3ObjectSummary> getListOfAllObjects(String bucketName) {
		AWSCredentialsProvider awsCredentialsProvider = getAWSCredentials();
		AmazonS3Client amazonS3Client = new AmazonS3Client(awsCredentialsProvider);	
		
		ObjectListing objectListing = amazonS3Client.listObjects(bucketName);
		
		List<S3ObjectSummary> s3ObjectSummeryList = objectListing.getObjectSummaries();
		
		return s3ObjectSummeryList;
	}
	
	public static void deleteBucket(String bucketName) {
		AWSCredentialsProvider awsCredentialsProvider = getAWSCredentials();
		AmazonS3Client amazonS3Client = new AmazonS3Client(awsCredentialsProvider);	
		
		amazonS3Client.deleteBucket(bucketName);
	}
	
	public static void deleteObject(String bucketName, String key) {
		
		AWSCredentialsProvider awsCredentialsProvider = getAWSCredentials();
		AmazonS3Client amazonS3Client = new AmazonS3Client(awsCredentialsProvider);	
		
		amazonS3Client.deleteObject(bucketName, key);
	}

}
