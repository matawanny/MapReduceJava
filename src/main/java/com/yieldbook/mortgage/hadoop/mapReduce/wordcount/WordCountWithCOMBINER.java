package com.yieldbook.mortgage.hadoop.mapReduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountWithCOMBINER {
	public static void main(String[] args) throws Exception {
		// creating object of configuration class
		Configuration conf = new Configuration();
		String[] In_Out_files = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		Path input_path = new Path(In_Out_files[0]);
		Path output_directory = new Path(In_Out_files[1]);

		Job job_obj = new Job(conf, "Word Count With Combiner");
		// setting name of main class
		job_obj.setJarByClass(WordCountWithCOMBINER.class);
		// name of mapper class
		job_obj.setMapperClass(WC_Mapper.class);
		// name of reducer class
		job_obj.setReducerClass(WC_Reducer.class);
		// name of combiner class
		job_obj.setCombinerClass(WC_Combiner.class);
		job_obj.setOutputKeyClass(Text.class);
		job_obj.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job_obj, input_path);

		FileOutputFormat.setOutputPath(job_obj, output_directory);

		output_directory.getFileSystem(job_obj.getConfiguration()).delete(
				output_directory, true);
		System.exit(job_obj.waitForCompletion(true) ? 0 : 1);
	}

	public static class WC_Mapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words_array = line.split(",");
			for (String new_word : words_array) {
				Text KEY_output = new Text(new_word.toUpperCase().trim());
				IntWritable VALUE_output = new IntWritable(1);
				// output
				con.write(KEY_output, VALUE_output);
			}
		}
	}

	public static class WC_Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> value_list,
				Context con) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable new_value : value_list) {
				sum = sum + new_value.get();
			}
			// output
			con.write(word, new IntWritable(sum));
		}
	}

	public static class WC_Combiner extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> value_list,
				Context con) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable new_value : value_list) {
				sum = sum + new_value.get();
			}
			con.write(word, new IntWritable(sum)); // output
		}
	}
}
