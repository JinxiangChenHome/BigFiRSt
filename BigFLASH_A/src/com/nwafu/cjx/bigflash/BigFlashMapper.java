package com.nwafu.cjx.bigflash;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import com.nwafu.cjx.fastdoop.QRecord;
import com.nwafu.cjx.utils.FlashJni;

/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */

public class BigFlashMapper extends Mapper<NullWritable,QRecord,IntWritable,Text>{
	int identificador;
	int jobID;
	String tmpDir;
	String outputDir = "";
	String tmpFileString = "";
	File fout;
	FileOutputStream fos;
	BufferedWriter bw;
	
	String tmpFileString2 = "";
	File fout2;
	FileOutputStream fos2;
	BufferedWriter bw2;
	
	File outputFile;
	
	String outputFileName = "";
	
	String[] initValues;
	String[] initQuality;
	
	String min_overlap = "";
	String max_overlap = "";
	String max_mismatch_density = "";
	String allow_outies = "";
	String phred_offset = "";
	String read_len = "";
	String fragment_len = "";
	String fragment_len_stddev = "";
	String interleaved = "";
	String tab_delimited_input = "";
	String tab_delimited_output = "";
	String output_prefix = "";
	String output_directory = "";
	String to_stdout = "";
	String compress = "";
	String threads = "";
	String quiet = "";
	String help = "";
	String version = "";
	String cap_mismatch_quals = "";
	String interleaved_input = "";
	String interleaved_output = "";
	String compress_prog = "";
	String compress_prog_args = "";
	String suffix = "";
	String output_suffix = "";
	String masterHostname = "";
	String clusterPort = "";
	
	boolean flagend = false;
	
	@Override
	protected void setup(Context context){
		identificador = context.getTaskAttemptID().getTaskID().getId();
		jobID = context.getJobID().getId();

		Configuration conf = context.getConfiguration();

		tmpDir = conf.get("hadoop.tmp.dir");
		this.outputDir = conf.get("outputGenomics");
		
		if(conf.get("min_overlap") != null){
			this.min_overlap = conf.get("min_overlap");
		}
		if(conf.get("max_overlap") != null){
			this.max_overlap = conf.get("max_overlap");
		}
		if(conf.get("max_mismatch_density") != null){
			this.max_mismatch_density = conf.get("max_mismatch_density");
		}
		if(conf.get("allow_outies") != null){
			this.allow_outies = conf.get("allow_outies");
		}
		if(conf.get("phred_offset") != null){
			this.phred_offset = conf.get("phred_offset");
		}
		if(conf.get("read_len") != null){
			this.read_len = conf.get("read_len");
		}
		if(conf.get("fragment_len") != null){
			this.fragment_len = conf.get("fragment_len");
		}
		if(conf.get("fragment_len_stddev") != null){
			this.fragment_len_stddev = conf.get("fragment_len_stddev");
		}
		if(conf.get("interleaved") != null){
			this.interleaved = conf.get("interleaved");
		}
		if(conf.get("tab_delimited_input") != null){
			this.tab_delimited_input = conf.get("tab_delimited_input");
		}
		if(conf.get("tab_delimited_output") != null){
			this.tab_delimited_output = conf.get("tab_delimited_output");
		}
		if(conf.get("output_prefix") != null){
			this.output_prefix = conf.get("output_prefix");
		}
		if(conf.get("output_directory") != null){
			this.output_directory = conf.get("output_directory");
		}
		if(conf.get("to_stdout") != null){
			this.to_stdout = conf.get("to_stdout");
		}
		if(conf.get("compress") != null){
			this.compress = conf.get("compress");
		}
		if(conf.get("threads") != null){
			this.threads = conf.get("threads");
		}
		if(conf.get("quiet") != null){
			this.quiet = conf.get("quiet");
		}
		if(conf.get("help") != null){
			this.help = conf.get("help");
		}
		if(conf.get("version") != null){
			this.version = conf.get("version");
		}
		if(conf.get("cap_mismatch_quals") != null){
			this.cap_mismatch_quals = conf.get("cap_mismatch_quals");
		}
		if(conf.get("interleaved_input") != null){
			this.interleaved_input = conf.get("interleaved_input");
		}
		if(conf.get("interleaved_output") != null){
			this.interleaved_output = conf.get("interleaved_output");
		}
		if(conf.get("compress_prog") != null){
			this.compress_prog = conf.get("compress_prog");
		}
		if(conf.get("compress_prog_args") != null){
			this.compress_prog_args = conf.get("compress_prog_args");
		}
		if(conf.get("suffix") != null){
			this.suffix = conf.get("suffix");
		}
		if(conf.get("output_suffix") != null){
			this.output_suffix = conf.get("output_suffix");
		}
		if(conf.get("masterHostname") != null){
			this.masterHostname = conf.get("masterHostname");
		}
		if(conf.get("clusterPort") != null){
			this.clusterPort = conf.get("clusterPort");
		}
		
		
		File tmpFile = new File(tmpDir);

		tmpFileString = tmpDir+"/HadoopTMPFile-"+identificador+"-"+String.valueOf(jobID)+".fastq";
		fout = new File(tmpFileString);
		try {
			fos = new FileOutputStream(fout);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		bw = new BufferedWriter(new OutputStreamWriter(fos));
		
		tmpFileString2 = tmpDir+"/HadoopTMPFile-"+identificador+"_2"+"-"+String.valueOf(jobID)+".fastq";
		fout2 = new File(tmpFileString2);
		try {
			fos2 = new FileOutputStream(fout2);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		bw2 = new BufferedWriter(new OutputStreamWriter(fos2));
	}
	
	String header;
	String sequence;
	String splitKey;
	String quality;
	
	@Override
	protected void map(NullWritable key, QRecord value, Context context)
			throws IOException, InterruptedException {
		
			header = value.getKey().trim();
			sequence = value.getValue().trim();
			splitKey = value.getKey2().trim();
			quality = value.getQuality().trim();
			
			if(sequence.startsWith("@")){
				flagend = true;
			}else {
				flagend = false;
			}

			if(!flagend){
				initValues = sequence.split(",SEQ,");
				
				initQuality = quality.split(",SEQ,");

				bw.write("@" + header);
				bw.newLine();
				
				bw.write(initValues[0]);
				bw.newLine();
				
				bw.write("+");
				bw.newLine();
				
				bw.write(initQuality[0]);
				bw.newLine();
				
				bw2.write("@" + header);
				bw2.newLine();
				
				bw2.write(initValues[1]);
				bw2.newLine();
				
				bw2.write("+");
				bw2.newLine();
				
				bw2.write(initQuality[1]);
				bw2.newLine();
					
				header = null;
				splitKey = null;
				sequence = null;
				quality = null;
				initValues = null;
				initQuality = null;
				
			}
			
	}
	
	@Override
	protected void cleanup(Context context) throws InterruptedException {
		try{
			bw.close();
			bw2.close();
			
			this.flash(0);
			
			String projectPath = System.getProperty("user.dir");
			if(this.suffix != ""){
				outputFileName = projectPath + "/out.extendedFrags.fastq." + this.suffix;
			}else {
				outputFileName = projectPath + "/out.extendedFrags.fastq";
				
			}
			outputFile = new File(outputFileName);

			Configuration configuration =new Configuration();
			FileSystem fs =FileSystem.get(new URI("hdfs://"+masterHostname+":"+clusterPort), configuration);

			fs.copyFromLocalFile(true,new Path(outputFileName), new Path(this.outputDir+"/file/Output"+this.identificador+".out"));
			
			fout2.delete();

			
			outputFile.delete();

			fout.delete();
			
			context.write(new IntWritable(this.identificador), new Text(outputDir+"/file/Output"+this.identificador+".out"));
			
			}
			catch(Exception e){
				e.printStackTrace();
				
				fout.delete();
				fout2.delete();
				outputFile = new File(outputFileName);
				outputFile.delete();
			}
	}
	
	private String[] parseParameters(int flashStep) {
		ArrayList<String> parameters = new ArrayList<String>();

		parameters.add("flash");

		parameters.add(this.tmpFileString);
		
		parameters.add(this.tmpFileString2);
		
		if(this.min_overlap != ""){
			parameters.add("-m");
			
			parameters.add(this.min_overlap);
		}
		if(this.max_overlap != ""){
			parameters.add("-M");
			
			parameters.add(this.max_overlap);
		}
		if(this.max_mismatch_density != ""){
			parameters.add("-x");
			
			parameters.add(this.max_mismatch_density);
		}
		if(this.allow_outies != ""){
			parameters.add("-O");
		}
		if(this.phred_offset != ""){
			parameters.add("-p");
			
			parameters.add(this.phred_offset);
		}
		if(this.read_len != ""){
			parameters.add("-r");
			
			parameters.add(this.read_len);
		}
		if(this.fragment_len != ""){
			parameters.add("-f");
			
			parameters.add(this.fragment_len);
		}
		if(this.fragment_len_stddev != ""){
			parameters.add("-s");
			
			parameters.add(this.fragment_len_stddev);
		}
		if(this.interleaved != ""){
			parameters.add("-I");
		}
		if(this.tab_delimited_input != ""){
			parameters.add("-Ti");
		}
		if(this.tab_delimited_output != ""){
			parameters.add("-To");
		}
		if(this.output_prefix != ""){
			parameters.add("-o");
			
			parameters.add(this.output_prefix);
		}
		if(this.output_directory != ""){
			parameters.add("-d");
			
			parameters.add(this.output_directory);
		}
		if(this.to_stdout != ""){
			parameters.add("-c");
			
			parameters.add(this.to_stdout);
		}
		if(this.compress != ""){
			parameters.add("-z");
		}
		if(this.threads != ""){
			parameters.add("-t");
			
			parameters.add(this.threads);
		}
		if(this.quiet != ""){
			parameters.add("-q");
		}
		if(this.help != ""){
			parameters.add("-h");
		}
		if(this.version != ""){
			parameters.add("-v");
		}
		if(this.cap_mismatch_quals != ""){
			parameters.add("--cap-mismatch-quals");
		}
		if(this.interleaved_input != ""){
			parameters.add("--interleaved-input");
		}
		if(this.interleaved_output != ""){
			parameters.add("--interleaved-output");
		}
		if(this.compress_prog != ""){
			parameters.add("--compress-prog");
			
			parameters.add(this.compress_prog);
		}
		if(this.compress_prog_args != ""){
			parameters.add("-compress-prog-args");
			
			parameters.add(this.compress_prog_args);
		}
		if(this.suffix != ""){
			parameters.add("--suffix");
			
			parameters.add(this.suffix);
		}
		if(this.output_suffix != ""){
			parameters.add("--output-suffix");
			
			parameters.add(this.output_suffix);
		}
		
		String[] parametersArray = new String[parameters.size()];

		return parameters.toArray(parametersArray);
	}
	
	public int flash(int flashStep) {
		String[] parametersArray = parseParameters(flashStep);

		int returnCode = FlashJni.Flash_Jni(parametersArray);

		if (returnCode != 0) {
			
			return returnCode;
		}

		return 0;
	}
}
