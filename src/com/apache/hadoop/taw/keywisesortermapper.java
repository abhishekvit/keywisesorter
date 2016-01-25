package com.apache.hadoop.taw;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class keywisesortermapper extends Mapper<LongWritable, Text, Text, Text>
{
	//int c=0;
	public void map(LongWritable mapperInputKey, Text mapperInputValue,
			Context context) throws IOException, InterruptedException 
	{
		String line = mapperInputValue.toString();
		String[] values = line.split(",");
		int c=0;
		for(String value:values)
		{
			if(c>0)
			{
				context.write(new Text(values[0]), new Text(value));
			}
			c++;
		}
	}

}