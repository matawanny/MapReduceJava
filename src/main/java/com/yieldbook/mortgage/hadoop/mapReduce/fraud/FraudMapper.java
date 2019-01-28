package com.yieldbook.mortgage.hadoop.mapReduce.fraud;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//GGYZ333519YS,Allison,01-01-2017,03-01-2017,Fedx,06-01-2017,no,null,null
public class FraudMapper extends Mapper<LongWritable, Text, Text, FraudWritable>
{

    private Text custId = new Text();    //  GGYZ333519YS 
    private FraudWritable data = new FraudWritable();    // will create object of writable class and initialize it

    @Override
    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

	
	String line = value.toString();    //GGYZ333519YS,Allison,01-01-2017,03-01-2017,Fedx,06-01-2017,no,null,null
	/* Split csv string */
	String[] words = line.split(",");  // [{GGYZ333519YS} {Allison} {01-01-2017} {03-01-2017} {Fedx} {06-01-2017} {no} {null} {null}]

	custId.set(words[0]);             // custdid = GGYZ333519YS
	
	data.set(words[1], words[5], words[6], words[7]);
	
	c.write(custId, data);
   //  GGYZ333519YS, Allison no null null
    }
}
