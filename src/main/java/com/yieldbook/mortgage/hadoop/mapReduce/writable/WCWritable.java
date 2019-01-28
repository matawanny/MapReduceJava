package com.yieldbook.mortgage.hadoop.mapReduce.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

public class WCWritable implements WritableComparable<WCWritable>
{
    private String word;
    
    public WCWritable()
    {
	set("");
    }
    
    public WCWritable(String word) 
    {
        set(word);
    }
     /* getters and setters for custom values */
    public void set(String word)
    {
	this.word = word;
    }

    public String getWord() 
    {
    	return this.word;
    }
    
     //Mandatory functions for Hadoop to know how to write values for this class//
    @Override
    public void write(DataOutput out) throws IOException
    {
	WritableUtils.writeString(out, this.word);
    }
    
    // Mandatory function for Hadoop to know how to read values for this class
         @Override
    public void readFields(DataInput in) throws IOException
    {
	this.word = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(WCWritable o) 
    {
        return this.word.compareTo(o.getWord());
    }
    
    @Override
    public String toString() 
    {
        return this.word;
    }
}
