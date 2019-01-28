package com.yieldbook.mortgage.hadoop.mapReduce.fraud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FraudDetectionDriver
{
    public static void main(String[] args) throws IOException,  ClassNotFoundException,	 InterruptedException
    {

/*	Path inputPath = new Path("hdfs://localhost:9000/user/jivesh/fraud");
	Path outputDir = new Path("hdfs://localhost:9000/user/jivesh/output");*/
    	
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
	
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Fraud Detection");

	job.setJarByClass(FraudDetectionDriver.class);

	job.setMapperClass(FraudMapper.class);
	job.setReducerClass(FraudReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FraudWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputDir);
	
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
  	
