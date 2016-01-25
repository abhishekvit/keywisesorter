package com.apache.hadoop.taw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class keywisesorterreducer extends Reducer<Text, Text,Text, Text> 

{
	
	private ArrayList<Integer> value = new ArrayList<Integer>();
	public void reduce(Text key, Iterable<Text> values,
    Context context) throws IOException, InterruptedException 
    {
		value.clear();
		for (Text val : values) 
			{
				value.add(Integer.parseInt(val.toString()));
			}
			Collections.sort(value);
			String str="";
			for (Integer number : value) 
			{
				   str=str+" "+Integer.toString(number);
			}
			context.write(key, new Text(str));
		}
		
		//context.write(key, result);
}
