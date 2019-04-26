package com.nwafu.cjx.bigflash;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class BigFlashReducer2 extends Reducer<IntWritable,Text,NullWritable,Text>{
	
	private HashMap<Integer,String> inputFiles;
	private List<Integer> inputFilesKeyList;
	FileSystem fs = null;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		this.inputFiles = new HashMap<Integer,String>();
		
		this.inputFilesKeyList = new ArrayList<Integer>();
		
		Configuration conf = context.getConfiguration();
		
		try {
			fs = FileSystem.get(new URI("hdfs://"+conf.get("masterHostname")+":"+conf.get("clusterPort")), conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try{
			for (Text val : values) {
				inputFiles.put(key.get(), val.toString());
				
				inputFilesKeyList.add(key.get());
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		Configuration configuration =context.getConfiguration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI("hdfs://"+configuration.get("masterHostname")+":"+configuration.get("clusterPort")), configuration);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		boolean readHeader = true;
		String currentFile ;
		String outCurrentFile ;
		FSDataInputStream fis ;
		InputStreamReader isReader;
		BufferedReader d ;
		String line;

		for(Integer i : inputFilesKeyList){

			currentFile = this.inputFiles.get(i);
			
			fis = new FSDataInputStream(fs.open(new Path(currentFile)));
			
			isReader = new InputStreamReader(fis);

			d = new BufferedReader(isReader);

			line = "";
			
			outCurrentFile = currentFile.substring(currentFile.indexOf("Output"), currentFile.length());
			
			while ((line = d.readLine())!=null) {
					
					if((line.startsWith("@") && readHeader) || (!line.startsWith("@")) ){
					
					context.write(NullWritable.get(), new Text(line));
					
				}
			}

			readHeader = false;

			d.close();

			fs.delete(new Path(currentFile), true);

		}
		
	}
}
