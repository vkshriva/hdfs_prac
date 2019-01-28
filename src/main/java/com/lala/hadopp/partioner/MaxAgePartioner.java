package com.lala.hadopp.partioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxAgePartioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {

		String token = value.toString();
		String contents[] = token.split("\t");
		int age = Integer.parseInt(contents[2]);

		if(age<=20)
		{
			return 0;
		}
		else if(age>20 && age<=30)
		{
			return 1 % numPartitions;
		}
		else
		{
			return 2 % numPartitions;
		}

	}

}
