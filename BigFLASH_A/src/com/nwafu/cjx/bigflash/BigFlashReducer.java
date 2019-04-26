package com.nwafu.cjx.bigflash;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.nwafu.cjx.common.ClientInfo;

/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class BigFlashReducer extends Reducer<IntWritable,Text,Text,Text> implements ClientInfo{
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			context.write(new Text(Integer.toString(key.get())), val);
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
	}
}
