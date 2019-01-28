package com.lala.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumberOfWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	int lineNumber =1;

	@Override
	public void map(LongWritable key, Text value,  Context context) throws IOException, InterruptedException {

		String contents = value.toString();
		String str[] = contents.split(" ");
		int noOfWords = str.length;

		context.write(new Text(lineNumber+" Line:"), new IntWritable(noOfWords));
		lineNumber++;

	}

}