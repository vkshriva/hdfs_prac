package com.lala.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxAgeReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		int max =-1;
		for (Text val : values) {
			String tokens  = val.toString();
			String content[] = tokens.split("\t");
			int newMax = Integer.parseInt(content[4]);
			if( newMax>max) {
				max=newMax;
			}

		}

		context.write(key, new IntWritable(max));

	}
}