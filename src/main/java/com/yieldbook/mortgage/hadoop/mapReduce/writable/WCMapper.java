package com.yieldbook.mortgage.hadoop.mapReduce.writable;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper
    extends Mapper<LongWritable, Text, WCWritable, IntWritable>{

    private final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context c)
	throws IOException, java.lang.InterruptedException{

	String line = value.toString();
	String[] words = line.split(",");
	for (String word : words){
	    c.write(new WCWritable(word), ONE);
	}
    }
}