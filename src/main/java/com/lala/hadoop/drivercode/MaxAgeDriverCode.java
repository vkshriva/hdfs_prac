package com.lala.hadoop.drivercode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.lala.hadoop.mapper.MaxAgeMapper;
import com.lala.hadoop.reducer.MaxAgeReducer;
import com.lala.hadopp.partioner.MaxAgePartioner;



public class MaxAgeDriverCode  {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(MaxAgeDriverCode.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MaxAgeMapper.class);
		//job.setCombinerClass(MaxAgeReducer.class);
		job.setReducerClass(MaxAgeReducer.class);
		job.setPartitionerClass(MaxAgePartioner.class);

		job.setNumReduceTasks(3);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit(0);
		}
		else {
			System.exit(1);
		}
	}

}
