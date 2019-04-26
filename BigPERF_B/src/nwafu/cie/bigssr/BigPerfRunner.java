package nwafu.cie.bigssr;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import nwafu.cie.common.PerfOptions;
import nwafu.cie.fastdoop.FASTAshortInputFileFormat;
import nwafu.cie.utils.Analyse_perf;
import nwafu.cie.utils.PerfUtils;

/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class BigPerfRunner extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		PerfOptions perfOptions = new PerfOptions(args);
		perfOptions = PerfUtils.getArgs(args,perfOptions);
		
		Configuration configuration = this.getConf();
		configuration.set("outputGenomics",perfOptions.getOutputPath());
		
		if(perfOptions.getMasterHostname() != "localhost"){
			configuration.set("masterHostname",perfOptions.getMasterHostname());
		}
		if(perfOptions.getClusterPort() != "8020"){
			configuration.set("clusterPort",perfOptions.getClusterPort());
		}
		
		// Get job
		Job job = Job.getInstance(configuration,"BigPerf");
		
		// Set main class
		job.setJarByClass(BigPerfRunner.class);
		// Set mapreduce class
		job.setMapperClass(BigPerfMapper.class);
		
		if(perfOptions.getNumReduceTasks() == ""){
			job.setNumReduceTasks(0);
		}else {
			job.setNumReduceTasks(Integer.parseInt(perfOptions.getNumReduceTasks()));
			job.setReducerClass(BigPerfReducer.class);
			// Set reduce output
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
		}
		
		job.setOutputFormatClass(TextOutputFormat.class);
		// Set map output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		// Set input and output
		FileInputFormat.setInputPaths(job, new Path(perfOptions.getInputPath()));
		FileOutputFormat.setOutputPath(job, new Path(perfOptions.getOutputPath()));
		// Custom Inputformat
		job.setInputFormatClass(FASTAshortInputFileFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		
		conf.setStrings("args", args);
		
		PerfOptions perfOptions = new PerfOptions(args);
		perfOptions = PerfUtils.getArgs(args,perfOptions);
		
		FileSystem fs = FileSystem.get(new URI("hdfs://"+perfOptions.getMasterHostname()+":"+perfOptions.getClusterPort()),conf);
		Path path = new Path(perfOptions.getOutputPath());
		//Delete directory if output path exists
		if(fs.exists(path)){
			fs.delete(path, true);
		}
		int res = ToolRunner.run(conf, new BigPerfRunner(), args);
		long endTime = System.currentTimeMillis();
		System.out.println("Total running time of the program：" + (endTime - startTime) + " ms");
		
		Analyse_perf analyse_perf = new Analyse_perf();
		if(perfOptions.getAnalyse()!=""){
			analyse_perf.analyse(perfOptions, conf);
		}
		System.exit(res);
	}
}