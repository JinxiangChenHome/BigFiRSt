package nwafu.cie.bigssr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import nwafu.cie.common.ClientInfo;
import nwafu.cie.common.PerfOptions;
import nwafu.cie.fastdoop.QRecord;
import nwafu.cie.utils.PerfUtils;

/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class BigPerfMapper2 extends Mapper<NullWritable,QRecord,Text,NullWritable> implements ClientInfo{
	
	public int min_seq_length;
	public int max_seq_length;
	Set<String> repeat_set;
	public String[] values;
	public String[] args;
	public PerfOptions perfOptions;
	public Set<String> filter_seq_ids_set = null;
	public Set<String> target_seq_ids_set = null;
	public String filter_seq_ids = "";
	public String target_seq_ids = "";
	String masterHostname = "";
	String clusterPort = "";
	public String flank_len = "-1";
	
	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		
		if(conf.get("masterHostname") != null){
			this.masterHostname = conf.get("masterHostname");
		}
		if(conf.get("clusterPort") != null){
			this.clusterPort = conf.get("clusterPort");
		}
		
		args = conf.getStrings("args");
		perfOptions = new PerfOptions(args);
		perfOptions = PerfUtils.getArgs(args,perfOptions);
		
		if(perfOptions.getFlank_len() != ""){
			this.flank_len = perfOptions.getFlank_len();
		}
		
		if(perfOptions.getFilter_seq_ids() != ""){
			this.filter_seq_ids = perfOptions.getFilter_seq_ids();
			filter_seq_ids_set = new HashSet<String>();
			try{
				FileSystem fileSystem = FileSystem.get(new URI("hdfs://"+masterHostname+":"+clusterPort), conf);
				Path path =new Path(perfOptions.getFilter_seq_ids());
				FSDataInputStream fsiStream = fileSystem.open(path);
		        BufferedReader reader = null;  
		        try {  
		            reader = new BufferedReader(new InputStreamReader(fsiStream));  
		            String tempString = null;  
		            while ((tempString = reader.readLine()) != null) {
		            	filter_seq_ids_set.add(tempString);
		            }
		            reader.close();  
		        } catch (IOException e3) {  
		            e3.printStackTrace();  
		        } finally {  
		            if (reader != null) {  
		                try {  
		                    reader.close();  
		                } catch (IOException e1) {  
		                }  
		            }  
		        }
			} catch (Exception e2) {
				System.out.println("Filter file specified is not found. Please provide a valid file");
				System.exit(1);
			}	
		} 
		else if(perfOptions.getTarget_seq_ids() != ""){
			this.target_seq_ids = perfOptions.getTarget_seq_ids();
			target_seq_ids_set = new HashSet<String>();
			try{
				FileSystem fileSystem = FileSystem.get(new URI("hdfs://"+masterHostname+":"+clusterPort), conf);
				Path path =new Path(perfOptions.getTarget_seq_ids());
				FSDataInputStream fsiStream = fileSystem.open(path);
		        BufferedReader reader = null;  
		        try {  
		            reader = new BufferedReader(new InputStreamReader(fsiStream));  
		            String tempString = null;  
		            while ((tempString = reader.readLine()) != null) {
		            	target_seq_ids_set.add(tempString);
		            }
		            reader.close();  
		        } catch (IOException e3) {  
		            e3.printStackTrace();  
		        } finally {  
		            if (reader != null) {  
		                try {  
		                    reader.close();  
		                } catch (IOException e1) {  
		                }  
		            }  
		        }
			} catch (Exception e2) {
				System.out.println("Filter file specified is not found. Please provide a valid file");
				System.exit(1);
			}	
		}
		
		if(perfOptions.getRepeats_path() != ""){
			try{
				FileSystem fileSystem = FileSystem.get(new URI("hdfs://"+masterHostname+":"+clusterPort), conf);
				Path path =new Path(perfOptions.getRepeats_path());
				FSDataInputStream fsiStream = fileSystem.open(path);
	
		        BufferedReader reader = null;  
		        try {  
		            reader = new BufferedReader(new InputStreamReader(fsiStream));  
		            String tempString = null;  
		            while ((tempString = reader.readLine()) != null) {
		                generated_repeats.add(tempString);
		            }  
		            perfOptions.setRepeats(generated_repeats);
		            reader.close();  
		        } catch (IOException e3) {  
		            e3.printStackTrace();  
		        } finally {  
		            if (reader != null) {  
		                try {  
		                    reader.close();  
		                } catch (IOException e1) {  
		                }  
		            }  
		        }
			} catch (Exception e2) {
				System.out.println("Repeats file specified is not found. Please provide a valid file");
				System.exit(1);
			}	
		}else {
			int min_motif_size = Integer.parseInt(perfOptions.getMin_motif_size());
			int max_motif_size = Integer.parseInt(perfOptions.getMax_motif_size());
			perfOptions.setRepeats(PerfUtils.generate_repeats(min_motif_size, max_motif_size));
		}
		
		if(perfOptions.getMin_length() != ""){
			int length_cutoff = Integer.parseInt(perfOptions.getMin_length());
			PerfUtils.build_rep_set(generated_repeats, length_cutoff);
		}
		else if (perfOptions.getMin_units() != "") {
			
			Map<Integer, Integer> unit_cutoff = new HashMap<Integer, Integer>();
			try {
				unit_cutoff.put(0, Integer.parseInt(perfOptions.getMin_units()));
			} catch (Exception e) {
				try {
					FileSystem fileSystem = FileSystem.get(new URI("hdfs://"+masterHostname+":"+clusterPort), conf);
					Path path =new Path(perfOptions.getMin_units());
					FSDataInputStream fsiStream = fileSystem.open(path);

			        BufferedReader reader = null;  
			        try {  
			            reader = new BufferedReader(new InputStreamReader(fsiStream));  
			            String tempString = null;  
			            String[] L;
			            Integer[] L2 = new Integer[2];
			            while ((tempString = reader.readLine()) != null) {
			                L = tempString.split("\t");
			                try {
								L2[0] = Integer.parseInt(L[0]);
								L2[1] = Integer.parseInt(L[1]);
								if(L2[1] == 1){
									System.out.println("Warning: Repeat unit of 1 used for size " + L2[0]);
								}
								unit_cutoff.put(L2[0], L2[1]);
							} catch (Exception e2) {
								System.out.println("Invalid file format given for minimum units. Refer to help for more details");
								System.exit(1);
							}
			                
			            }  
			            reader.close();  
			        } catch (IOException e3) {  
			            e3.printStackTrace();  
			        } finally {  
			            if (reader != null) {  
			                try {  
			                    reader.close();  
			                } catch (IOException e1) {  
			                }  
			            }  
			        }  
				} catch (Exception e2) {
					System.out.println("Units file specified is not found. Please provide a valid file");
					System.exit(1);
				}
			}
			
			PerfUtils.build_rep_set_unit(generated_repeats, unit_cutoff);
			
		}
		else if (perfOptions.getMin_units() == "" && perfOptions.getMin_length() == "") {
			perfOptions.setMin_length("12");
			int length_cutoff = Integer.parseInt(perfOptions.getMin_length());
			PerfUtils.build_rep_set(generated_repeats, length_cutoff);
		}
		
		
		this.min_seq_length = Integer.parseInt(perfOptions.getMin_seq_length());
		this.max_seq_length = Integer.parseInt(perfOptions.getMax_seq_length());
		
		this.repeat_set = repeats_out.keySet();
	}
	
	String header;
	String sequence;

	@Override
	protected void map(NullWritable key, QRecord value, Context context) throws IOException, InterruptedException{
		header = ">" + value.getKey();
		sequence = value.getValue();
		
		Pattern pattern = Pattern.compile("\\s*|\t|\r|\n");
		Matcher matcher = pattern.matcher(sequence);
		sequence = matcher.replaceAll("");
		
		if(min_seq_length <= sequence.length() && sequence.length() <= max_seq_length){
			if(filter_seq_ids_set == null && target_seq_ids_set == null){
				PerfUtils.get_ssrs(header,sequence, repeats_out, repeat_set,flank_len,context);
			}
			else if (filter_seq_ids_set != null) {
				if(!filter_seq_ids_set.contains(header)){
					PerfUtils.get_ssrs(header,sequence, repeats_out, repeat_set,flank_len,context);
				}
			}
			else if (target_seq_ids_set != null) {
				if(target_seq_ids_set.contains(header)){
					PerfUtils.get_ssrs(header,sequence, repeats_out, repeat_set,flank_len,context);
				}
			}
		}
		
		header = null;
		sequence = null;
	}
	
	@Override
	protected void cleanup(Context context) throws InterruptedException {
		
	}
	
	
}
