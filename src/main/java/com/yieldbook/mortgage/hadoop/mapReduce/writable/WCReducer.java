package com.yieldbook.mortgage.hadoop.mapReduce.writable;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer
    extends Reducer<WCWritable, IntWritable, WCWritable, IntWritable>{

    @Override
    protected void reduce(WCWritable key, Iterable<IntWritable> values, Context c)throws IOException,	java.lang.InterruptedException
    {
	int sum = 0;
	Iterator<IntWritable> valuesIter = values.iterator();
	while (valuesIter.hasNext()){
	    sum += valuesIter.next().get();
	}
	c.write(key, new IntWritable(sum));
    }
}
