package com.lala.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWord extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value,  Context context) throws IOException, InterruptedException {

		String contents = value.toString();
		String str[] = contents.split(" ");

		for (String val:str) {
			context.write(new Text(val), new IntWritable(1));
		}

	}

}