package com.nwafu.cjx.bigflash;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.nwafu.cjx.common.FlashOptions;
import com.nwafu.cjx.fastdoop.FASTQInputFileFormat;
/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class BigFlashRunner extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		FlashOptions flashOptions = new FlashOptions(args);
		
		Configuration configuration = this.getConf();
		configuration.set("outputGenomics",flashOptions.getHoutputPath());
		
		if(flashOptions.getMin_overlap() != "10"){
			configuration.set("min_overlap",flashOptions.getMin_overlap());
		}
		if(flashOptions.getMax_overlap() != "65"){
			configuration.set("max_overlap",flashOptions.getMax_overlap());
		}
		if(flashOptions.getMax_mismatch_density() != "0.25"){
			configuration.set("max_mismatch_density",flashOptions.getMax_mismatch_density());
		}
		if(flashOptions.getPhred_offset() != "33"){
			configuration.set("phred_offset",flashOptions.getPhred_offset());
		}
		if(flashOptions.getOutput_prefix() != "out"){
			configuration.set("output_prefix",flashOptions.getOutput_prefix());
		}
		if(flashOptions.getAllow_outies() != ""){
			configuration.set("allow_outies",flashOptions.getAllow_outies());
		}
		if(flashOptions.getRead_len() != ""){
			configuration.set("read_len",flashOptions.getRead_len());
		}
		if(flashOptions.getFragment_len() != ""){
			configuration.set("fragment_len",flashOptions.getFragment_len());
		}
		if(flashOptions.getFragment_len_stddev() != ""){
			configuration.set("fragment_len_stddev",flashOptions.getFragment_len_stddev());
		}
		if(flashOptions.getInterleaved() != ""){
			configuration.set("interleaved",flashOptions.getInterleaved());
		}
		if(flashOptions.getTab_delimited_input() != ""){
			configuration.set("tab_delimited_input",flashOptions.getTab_delimited_input());
		}
		if(flashOptions.getTab_delimited_output() != ""){
			configuration.set("tab_delimited_output",flashOptions.getTab_delimited_output());
		}
		if(flashOptions.getOutput_directory() != ""){
			configuration.set("output_directory",flashOptions.getOutput_directory());
		}
		if(flashOptions.getTo_stdout() != ""){
			configuration.set("to_stdout",flashOptions.getTo_stdout());
		}
		if(flashOptions.getCompress() != ""){
			configuration.set("compress",flashOptions.getCompress());
		}
		if(flashOptions.getThreads() != ""){
			configuration.set("threads",flashOptions.getThreads());
		}
		if(flashOptions.getQuiet() != ""){
			configuration.set("quiet",flashOptions.getQuiet());
		}
		if(flashOptions.getHelp() != ""){
			configuration.set("help",flashOptions.getHelp());
		}
		if(flashOptions.getVersion() != ""){
			configuration.set("version",flashOptions.getVersion());
		}
		if(flashOptions.getCap_mismatch_quals() != ""){
			configuration.set("cap_mismatch_quals",flashOptions.getCap_mismatch_quals());
		}
		if(flashOptions.getInterleaved_input() != ""){
			configuration.set("interleaved_input",flashOptions.getInterleaved_input());
		}
		if(flashOptions.getInterleaved_output() != ""){
			configuration.set("interleaved_output",flashOptions.getInterleaved_output());
		}
		if(flashOptions.getCompress_prog() != ""){
			configuration.set("compress_prog",flashOptions.getCompress_prog());
		}
		if(flashOptions.getCompress_prog_args() != ""){
			configuration.set("compress_prog_args",flashOptions.getCompress_prog_args());
		}
		if(flashOptions.getSuffix() != ""){
			configuration.set("suffix",flashOptions.getSuffix());
		}
		if(flashOptions.getOutput_suffix() != ""){
			configuration.set("output_suffix",flashOptions.getOutput_suffix());
		}
		if(flashOptions.getMasterHostname() != "localhost"){
			configuration.set("masterHostname",flashOptions.getMasterHostname());
		}
		if(flashOptions.getClusterPort() != "8020"){
			configuration.set("clusterPort",flashOptions.getClusterPort());
		}
		
		// Get job
		Job job = Job.getInstance(configuration,"BigFlash");
		// Set the main class
		job.setJarByClass(BigFlashRunner.class);
		// Set the mapreduce class
		job.setMapperClass(BigFlashMapper.class);
		if(flashOptions.getNumReduceTasks() == ""){
			job.setNumReduceTasks(0);
		}else {
			job.setNumReduceTasks(Integer.parseInt(flashOptions.getNumReduceTasks()));
			job.setReducerClass(BigFlashReducer2.class);
			// Set the reduce output
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
		}
		
		// Set map output
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		// Set input and output
		FileInputFormat.setInputPaths(job, new Path(flashOptions.getHinputPath()));
		FileOutputFormat.setOutputPath(job, new Path(flashOptions.getHoutputPath()));
		//Custom Inputformat
		job.setInputFormatClass(FASTQInputFileFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		
		FlashOptions flashOptions = new FlashOptions(args);
		
		FileSystem fs = FileSystem.get(new URI("hdfs://"+flashOptions.getMasterHostname()+":"+flashOptions.getClusterPort()),conf);
		Path path = new Path(flashOptions.getHoutputPath());
		//Delete directory if output path exists
		if(fs.exists(path)){
			fs.delete(path, true);
		}
		int res = ToolRunner.run(new Configuration(), new BigFlashRunner(), args);
		long endTime = System.currentTimeMillis();
		System.out.println("Total running time of the program：" + (endTime - startTime) + " ms");
		System.exit(res);
	}
}