package com.lala.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxAgeMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String tokens = value.toString();
		String contents[] = tokens.split("\t");
		Text newKey = new Text(contents[3]);

		context.write(newKey, value);

	}

}