package com.lifotech.aws.s3.client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.lifotech.aws.s3.S3Manager;

public class S3SampleClient {

	public static void main(String[] args) {

		try {

			String bucketName = "lifotech-us-bucket-1"; 

			System.out.println("==========================");
			System.out.println("Creating Bucket using Amazon S3");
			System.out.println("==========================");

			S3Manager.createBucket(bucketName);
			
			System.out.println(bucketName + " created. ");
			System.out.println("Listing buckets in your account");
			
			List<Bucket> bucketList = S3Manager.getBucketList();
			
			for (Bucket bucket: bucketList) {
				System.out.println("Bucket Name : " + bucket.getName());
			}
			
			System.out.println("uploading file  into bucket " + bucketName);
			
			String key = "testFile.txt";
			
			S3Manager.putObjectIntoBucket(bucketName, key, createFile());
			
			System.out.println(" file added into bucket");
			
			System.out.println("downloading the object");
			
			S3Manager.downLoadObjectFromBucket(bucketName, key, "C:\\temp\\");
			
			System.out.println("File downloaded from the bucket");
			
			
			System.out.println("Get list of all object in the bucket");
			
			List<S3ObjectSummary> list = S3Manager.getListOfAllObjects(bucketName);
			
			for(S3ObjectSummary objectSummary: list) {
				System.out.println("Object Name: " + objectSummary.getBucketName() + " and size: " +  objectSummary.getSize() + " bytes");
			}
				
			
			System.out.println("Deleting all object in the bucket ");
			
			for(S3ObjectSummary objectSummary: list) {
				System.out.println("Deleting  " + objectSummary.getKey());
				S3Manager.deleteObject(bucketName, objectSummary.getKey());
			}
						
			
			System.out.println("Delete  bucket ");
			
			S3Manager.deleteBucket(bucketName);
			
			System.out.println("bucket deleted ");
			

		} catch (AmazonServiceException e) {
			System.out.println("Caught AmazonServiceException which means your request made it to Amazon S3 "
					+ "however, it was rejetced with an error repsonse for some reason");
			System.out.println("AWS Error message " + e.getMessage());
			System.out.println("AWS Error Code " + e.getErrorCode());
			System.out.println("HTTP Status code " + e.getStatusCode());
			System.out.println("Error Type " + e.getErrorType());
			System.out.println("Request ID " + e.getRequestId());
		} catch (AmazonClientException e) {
			System.out.println("Caught AmazonServiceException which means your request either could not reach to Amazon S3 or "
					+ "response could not be parsed");
			System.out.println("AWS Error message " + e.getMessage());
		} catch (RuntimeException e) {
			System.out.println("Caught RuntimeExceptiont");
			System.out.println("Error message : " + e.getMessage());
		} catch (IOException e) {
			System.out.println("Caught IOException");
			System.out.println("Error message: " + e.getMessage() );
		}

	}

	private static File createFile() throws IOException {
		
		File file = new File("myTemp.txt");
		file.deleteOnExit();
		BufferedWriter out = new BufferedWriter(new FileWriter(file));
		
		out.write("This is fist line\n");
		
		out.write("This is second line\n");
		out.close();
		return file;
	}

}
