package nwafu.cie.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import nwafu.cie.common.ClientInfo;
import nwafu.cie.common.PerfOptions;
/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class PerfUtils implements ClientInfo{
	
	public static void get_ssrs(String seq_info,String record_seq, Map<String, Object> repeats_info, Set<String> repeat_set, String flank_len, Context context) throws IOException, InterruptedException{
		
		String seq_info_id = seq_info.split(" ")[0];
		
		List<Integer> repeat_lengths = (List<Integer>)repeats_info.get("rep_lengths");
		
		int input_seq_length = record_seq.length();
		String outline;
		/*int left_flank_len = -1;
		int right_flank_len = -1*/;
		
		for(int length_cutoff : repeat_lengths){
			int fallback = length_cutoff - 1;
			int sub_start = 0;  // substring start
			int sub_stop = sub_start + repeat_lengths.get(repeat_lengths.size()-1);  // substring stop
			
			/*if(flank_len != ""){
				left_flank_len = Integer.parseInt(flank_len);
				right_flank_len = input_seq_length - Integer.parseInt(flank_len);
			}*/
			int flank_len_tmp = Integer.parseInt(flank_len);
			
			int flank_skip = -1;
			int sub_start2 = 0;
			int sub_stop2 = sub_start2 + repeat_lengths.get(repeat_lengths.size()-1);
			int left_flank_len = flank_len_tmp;
			int right_flank_len = input_seq_length - flank_len_tmp;
			
			while (sub_stop2 <= (left_flank_len - 1)){
				sub_stop2 = sub_start2 + length_cutoff;
				if(sub_stop2 > (left_flank_len - 1)){
					break;
				}
			    String subseq = record_seq.substring(sub_start2, sub_stop2);
			    
				if(repeat_set.contains(subseq)){
					
					flank_skip = 1;
					
					boolean match = true;
					int motif_length = ((Map<String,Integer>)repeats_info.get(subseq)).get("motif_length");
					int offset = length_cutoff % motif_length;
					String repeat_seq = record_seq.substring(sub_start2+offset, sub_start2+offset+motif_length);
					int i = 0;
					while (match){
						int j = sub_stop2;
						
						if(sub_stop2 >= (left_flank_len - 1)){
							match = false;

							int match_length = sub_stop2 - sub_start2;
							int num_units = match_length/motif_length;

							sub_start2 = sub_stop2 - fallback;
						}
						else if(record_seq.charAt(j) == repeat_seq.charAt(i)){
							sub_stop2 += 1;
	                        i += 1;
	                        if (i >= motif_length){
	                        	i = 0;
	                        }
						}
						else {
							match = false;

							int match_length = sub_stop2 - sub_start2;
							int num_units = match_length/motif_length;
							sub_start2 = sub_stop2 - fallback;
							
						}
					}
				}else {
					sub_start2 += 1;
				}
			}
			
			if(flank_skip == 1){
				continue;
			}
			
			sub_start2 = right_flank_len + 1;
			sub_stop2 = sub_start2 + repeat_lengths.get(repeat_lengths.size()-1);
			
			while (sub_stop2 <= input_seq_length){
				sub_stop2 = sub_start2 + length_cutoff;
				if(sub_stop2 > input_seq_length){
					break;
				}
			    String subseq = record_seq.substring(sub_start2, sub_stop2);
			    
				if(repeat_set.contains(subseq)){
					
					flank_skip = 1;
					
					boolean match = true;
					int motif_length = ((Map<String,Integer>)repeats_info.get(subseq)).get("motif_length");
					int offset = length_cutoff % motif_length;
					String repeat_seq = record_seq.substring(sub_start2+offset, sub_start2+offset+motif_length);
					int i = 0;
					while (match){
						int j = sub_stop2;
						
						if(sub_stop2 >= input_seq_length){
							match = false;

							int match_length = sub_stop2 - sub_start2;
							int num_units = match_length/motif_length;

							sub_start2 = sub_stop2 - fallback;
						}
						else if(record_seq.charAt(j) == repeat_seq.charAt(i)){
							sub_stop2 += 1;
	                        i += 1;
	                        if (i >= motif_length){
	                        	i = 0;
	                        }
						}
						else {
							match = false;

							int match_length = sub_stop2 - sub_start2;
							int num_units = match_length/motif_length;
							sub_start2 = sub_stop2 - fallback;
							
						}
					}
				}else {
					sub_start2 += 1;
				}
			}
			
			if(flank_skip == 1){
				continue;
			}
			
			
			while (sub_stop <= input_seq_length){
				sub_stop = sub_start + length_cutoff;
				if(sub_stop > input_seq_length){
					break;
				}
			    String subseq = record_seq.substring(sub_start, sub_stop);
			    
				if(repeat_set.contains(subseq)){
					/*//Set flank_len
					if(right_flank_len != -1){
						if((sub_start < left_flank_len) || (sub_stop > right_flank_len)){
							break;
						}
					}*/
					
					boolean match = true;
					int motif_length = ((Map<String,Integer>)repeats_info.get(subseq)).get("motif_length");
					int offset = length_cutoff % motif_length;
					String repeat_seq = record_seq.substring(sub_start+offset, sub_start+offset+motif_length);
					int i = 0;
					while (match){
						int j = sub_stop;
						
						if(sub_stop >= input_seq_length){
							match = false;

							int match_length = sub_stop - sub_start;
							int num_units = match_length/motif_length;
							outline = seq_info_id + "  " + sub_start + "  " + sub_stop + "  " + ((Map<String,String>)repeats_info.get(subseq)).get("class") + "  " + match_length + "  " + ((Map<String,String>)repeats_info.get(subseq)).get("strand") + "  " + num_units + "  " + subseq.substring(0, motif_length);
							context.write(new Text(outline),NullWritable.get());
							
							sub_start = sub_stop - fallback;
						}
						else if(record_seq.charAt(j) == repeat_seq.charAt(i)){
							sub_stop += 1;
	                        i += 1;
	                        if (i >= motif_length){
	                        	i = 0;
	                        }
						}
						else {
							match = false;

							int match_length = sub_stop - sub_start;
							int num_units = match_length/motif_length;
							outline = seq_info_id + "  " + sub_start + "  " + sub_stop + "  " + ((Map<String,String>)repeats_info.get(subseq)).get("class") + "  " + match_length + "  " + ((Map<String,String>)repeats_info.get(subseq)).get("strand") + "  " + num_units + "  " + subseq.substring(0, motif_length);
							
							context.write(new Text(outline),NullWritable.get());
							
							sub_start = sub_stop - fallback;
							
						}
					}
				}else {
					sub_start += 1;
				}
			}
		}
	}
	
	public static void get_ssrs_units(String seq_info,String record_seq, Map<String, Object> repeats_info, Set<String> repeat_set, Context context) throws IOException, InterruptedException{
		
		String seq_info_id = seq_info.split(" ")[0];
		
		List<Integer> repeat_lengths = (List<Integer>)repeats_info.get("rep_lengths");
		
		int input_seq_length = record_seq.length();
		String outline;
		
		for(int length_cutoff : repeat_lengths){
			int fallback = length_cutoff - 1;
			int sub_start = 0;  // substring start
			int sub_stop = sub_start + repeat_lengths.get(repeat_lengths.size()-1);  // substring stop
			while (sub_stop <= input_seq_length){
				sub_stop = sub_start + length_cutoff;
				if(sub_stop > input_seq_length){
					break;
				}
			    String subseq = record_seq.substring(sub_start, sub_stop);
			    
				if(repeat_set.contains(subseq)){
					boolean match = true;
					int motif_length = ((Map<String,Integer>)repeats_info.get(subseq)).get("motif_length");
					int offset = length_cutoff % motif_length;
					String repeat_seq = record_seq.substring(sub_start+offset, sub_start+offset+motif_length);
					int i = 0;
					while (match){
						int j = sub_stop;
						
						if(sub_stop >= input_seq_length){
							match = false;
							int match_length = sub_stop - sub_start;
							int num_units = match_length/motif_length;
							outline = seq_info_id + "\t" + sub_start + "\t" + sub_stop + "\t" + ((Map<String,String>)repeats_info.get(subseq)).get("class") + "\t" + match_length + "\t" + ((Map<String,String>)repeats_info.get(subseq)).get("strand") + "\t" + num_units + "\t" + subseq.substring(0, motif_length);
							context.write(new Text(outline),NullWritable.get());
							
							sub_start = sub_stop - fallback;
						}
						else if(record_seq.charAt(j) == repeat_seq.charAt(i)){
							sub_stop += 1;
	                        i += 1;
	                        if (i >= motif_length){
	                        	i = 0;
	                        }
						}
						else {
							match = false;
							int match_length = sub_stop - sub_start;
							int num_units = match_length/motif_length;
							outline = seq_info_id + "\t" + sub_start + "\t" + sub_stop + "\t" + ((Map<String,String>)repeats_info.get(subseq)).get("class") + "\t" + match_length + "\t" + ((Map<String,String>)repeats_info.get(subseq)).get("strand") + "\t" + num_units + "\t" + subseq.substring(0, motif_length);
							context.write(new Text(outline),NullWritable.get());
							sub_start = sub_stop - fallback;
						}
					}
				}else {
					sub_start += 1;
				}
			}
		}
	}
	
	
	
	public static void get_ssrs_units_serial(String seq_info,String record_seq, Map<String, Object> repeats_info, Set<String> repeat_set, BufferedWriter out) throws IOException, InterruptedException{
		
		String seq_info_id = seq_info.split(" ")[0];
		
		List<Integer> repeat_lengths = (List<Integer>)repeats_info.get("rep_lengths");
		
		int input_seq_length = record_seq.length();
		String outline;
		
		for(int length_cutoff : repeat_lengths){
			int fallback = length_cutoff - 1;
			int sub_start = 0;  // substring start
			int sub_stop = sub_start + repeat_lengths.get(repeat_lengths.size()-1);  // substring stop
			while (sub_stop <= input_seq_length){
				sub_stop = sub_start + length_cutoff;
				if(sub_stop > input_seq_length){
					break;
				}
			    String subseq = record_seq.substring(sub_start, sub_stop);
			    
				if(repeat_set.contains(subseq)){
					boolean match = true;
					int motif_length = ((Map<String,Integer>)repeats_info.get(subseq)).get("motif_length");
					int offset = length_cutoff % motif_length;
					String repeat_seq = record_seq.substring(sub_start+offset, sub_start+offset+motif_length);
					int i = 0;
					while (match){
						int j = sub_stop;
						
						if(sub_stop >= input_seq_length){
							match = false;
							int match_length = sub_stop - sub_start;
							int num_units = match_length/motif_length;
							outline = seq_info_id + "\t" + sub_start + "\t" + sub_stop + "\t" + ((Map<String,String>)repeats_info.get(subseq)).get("class") + "\t" + match_length + "\t" + ((Map<String,String>)repeats_info.get(subseq)).get("strand") + "\t" + num_units + "\t" + subseq.substring(0, motif_length);
							try {
								out.write(outline);
								out.newLine();
							} catch (IOException e) {
								e.printStackTrace();
							}
							
							sub_start = sub_stop - fallback;
						}
						else if(record_seq.charAt(j) == repeat_seq.charAt(i)){
							sub_stop += 1;
	                        i += 1;
	                        if (i >= motif_length){
	                        	i = 0;
	                        }
						}
						else {
							match = false;
							int match_length = sub_stop - sub_start;
							int num_units = match_length/motif_length;
							outline = seq_info_id + "\t" + sub_start + "\t" + sub_stop + "\t" + ((Map<String,String>)repeats_info.get(subseq)).get("class") + "\t" + match_length + "\t" + ((Map<String,String>)repeats_info.get(subseq)).get("strand") + "\t" + num_units + "\t" + subseq.substring(0, motif_length);
							try {
								out.write(outline);
								out.newLine();
							} catch (IOException e) {
								e.printStackTrace();
							}
							sub_start = sub_stop - fallback;
						}
					}
				}else {
					sub_start += 1;
				}
			}
		}
	}
	
	
	public static void getSSRNative(PerfOptions perfOptions) {
		int length_cutoff = Integer.parseInt(perfOptions.getMin_length());
		
		
		String seq_file = perfOptions.getInputPath();
		String out_file = perfOptions.getOutputPath();
		build_rep_set(generated_repeats, length_cutoff);
		Set<String> repeat_set = repeats_out.keySet();
		int min_seq_length = Integer.parseInt(perfOptions.getMin_seq_length());
		int max_seq_length = Integer.parseInt(perfOptions.getMax_seq_length());
		
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(seq_file)));
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out_file),"GBK"));
			String seq_info = null;
			String record_seq = "";
			String str = in.readLine();
			while(str != null && str.charAt(0) == '>'){
				seq_info = str;
				record_seq = "";
				while ((str = in.readLine()) != null && str.charAt(0) != '>') {
					record_seq += str; 
		        }
				if(min_seq_length <= record_seq.length() && record_seq.length() <= max_seq_length){
					//get_ssrs(seq_info,record_seq, repeats_out, repeat_set, out);
				}
			}
			
	        out.flush();
	        in.close();
	        out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void getSSR_units_serial(PerfOptions perfOptions, Map<Integer, Integer> unit_cutoff) throws InterruptedException {
		String seq_file = perfOptions.getInputPath();
		String out_file = perfOptions.getOutputPath();
		build_rep_set_unit(generated_repeats, unit_cutoff);
		Set<String> repeat_set = repeats_out.keySet();
		int min_seq_length = Integer.parseInt(perfOptions.getMin_seq_length());
		int max_seq_length = Integer.parseInt(perfOptions.getMax_seq_length());
		
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(seq_file)));
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out_file),"GBK"));
			String seq_info = null;
			String record_seq = "";
			String str = in.readLine();
			while(str != null && str.charAt(0) == '>'){
				seq_info = str;
				record_seq = "";
				while ((str = in.readLine()) != null && str.charAt(0) != '>') {
					record_seq += str; 
		        }
				if(min_seq_length <= record_seq.length() && record_seq.length() <= max_seq_length){
					get_ssrs_units_serial(seq_info,record_seq, repeats_out, repeat_set, out);
				}
			}
			
	        out.flush();
	        in.close();
	        out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Map<String, Object> build_rep_set(List<String> repeat_file, int length_cutoff){
		
		Map<Integer, Integer> motif_fallback = new HashMap<Integer, Integer>();
		
		if(length_cutoff != -1){
			List<Integer> motif_lengths = new ArrayList<Integer>(); 
			for(String curLine : repeat_file){
				String[] line = curLine.split("\t");
				Map<String, Object> motif_dict = new HashMap<String, Object>();
				String motif = line[0];
				int motif_length = Integer.parseInt(line[2]);
				if(!motif_lengths.contains(motif_length)){
					motif_lengths.add(motif_length);
				}
				int i = 0;
				int input_seq_length = motif_length;
				while ((i < motif_length) &&(input_seq_length < length_cutoff)){
					motif += motif.charAt(i);
					i += 1;
					if(i >= motif_length){
						i = 0;
					}
					input_seq_length = motif.length();
				}
				motif_dict.put("class", line[1]);
				motif_dict.put("motif_length", motif_length);
				motif_dict.put("strand", line[3]);
				repeats_out.put(motif, motif_dict);
			}
			for(int m : motif_lengths){
				motif_fallback.put(m, ArrayListMax(motif_lengths));
			}
			repeats_out.put("fallback", motif_fallback);
			
			List<Integer> length_cutoff_list=new ArrayList<Integer>(){};
			length_cutoff_list.add(length_cutoff);
			
			repeats_out.put("rep_lengths", length_cutoff_list);
		}
		return repeats_out;
	}
	
	public static Map<String, Object> build_rep_set_unit(List<String> repeat_file, Map<Integer, Integer> unit_cutoff){
		
		Map<Integer, Integer> motif_fallback = new HashMap<Integer, Integer>();
		List<Integer> repeat_lengths = new ArrayList<Integer>(); 
		if(unit_cutoff != null){
			for(String curLine : repeat_file){
				String[] line = curLine.split("\t");
				Map<String, Object> motif_dict = new HashMap<String, Object>();
				String motif = line[0];
				int motif_length = Integer.parseInt(line[2]);
				String tmpMotif = motif;
				try {
					for (int i = 0; i < unit_cutoff.get(motif_length) - 1; i++) {
						motif += tmpMotif;
					}
				} catch (Exception e) {
					for (int i = 0; i < unit_cutoff.get(0) - 1; i++) {
						motif += tmpMotif;
					}
				}
				
				if(!repeat_lengths.contains(motif.length())){
					repeat_lengths.add(motif.length());
				}
				motif_fallback.put(motif_length, motif.length() - 1);
				motif_dict.put("class", line[1]);
				motif_dict.put("motif_length", motif_length);
				motif_dict.put("strand", line[3]);
				repeats_out.put(motif, motif_dict);
				
			}
			repeats_out.put("fallback", motif_fallback);
			
			Collections.sort(repeat_lengths);
			
			repeats_out.put("rep_lengths", repeat_lengths);
		}
		return repeats_out;
	}
	
	
	//Find the maximum value of ArrayList
	public static int ArrayListMax(List<Integer> sampleList) {
		try {
			int maxDevation = 0;
			int totalCount = sampleList.size();
			if (totalCount >= 1) {
				int max = Integer.parseInt(sampleList.get(0).toString());
				for (int i = 0; i < totalCount; i++) {
					int temp = Integer.parseInt(sampleList.get(i).toString());
					if (temp > max) {
						max = temp;
					}
				}
				maxDevation = max;
			}
			return maxDevation;
		} catch (Exception ex) {
			throw ex;
		}
	}
	
	
	public static PerfOptions getArgs(String[] args,PerfOptions perfOptions) {
		perfOptions = new PerfOptions(args);
		if(perfOptions.getRepeats() != null && (perfOptions.getMin_motif_size() != "" || perfOptions.getMax_motif_size() != "")){
			System.out.println("-rep is not allowed with -m/-M");
		}
		if(perfOptions.getRepeats() == null){
			if(perfOptions.getMin_motif_size() == ""){
				perfOptions.setMin_motif_size("1");
			}
			if(perfOptions.getMax_motif_size() == ""){
				perfOptions.setMax_motif_size("6");
			}
		}
		return perfOptions;
	}
	
	
	public static List<String> product(String[] alphabet,int repeat,String string){
		for (int i = 0; i < alphabet.length; i++) {
			if(repeat > 1){
				product(alphabet, repeat-1,string + alphabet[i]);
			}else {
				product_alphabet.add(string + alphabet[i]);
			}
		}
		return product_alphabet;
	}
	
	
	public static List<String> product_alphabet = new ArrayList<String>(){};
	
	public static String rev_comp(String combination){
		StringBuilder stringBuilder = new StringBuilder(combination);
		for (int i = stringBuilder.length() - 1; i >= 0; i--) {
			if(stringBuilder.charAt(i) == 'A'){
				stringBuilder.setCharAt(i, 'T');
			}else if(combination.charAt(i) == 'C'){
				stringBuilder.setCharAt(i, 'G');
			}else if(combination.charAt(i) == 'G'){
				stringBuilder.setCharAt(i, 'C');
			}else if(combination.charAt(i) == 'T'){
				stringBuilder.setCharAt(i, 'A');
			}
		}
		return stringBuilder.reverse().toString();
	}
	
	
	public static String expand_repeat(String combination, int max_motif_size){
		String return_string = "";
		int i = 0;
		while(return_string.length() < max_motif_size){
			return_string += combination.charAt(i);
			i += 1;
			if(i >= combination.length()){
				i = 0;
			}
		}
		return return_string;
	}
	
	
	public static List<String> cycles = null;
	
	public static List<String> get_cycles(String repeat){
		cycles = new ArrayList<String>();
		for(int i = 0;i < repeat.length();i++){
			cycles.add(repeat.substring(i) + repeat.substring(0, i));
		}
		return cycles;
	}
	
	public static List<String> generate_repeats(int min_motif_size , int max_motif_size){
		
		String[] alphabet = {"A","C","G","T"};
		String string = "";
		List<String> alphabet_repeat = null;
		
		Set expanded_set = new HashSet();
		Set repeat_set = new HashSet();
		
		String strand = null;
		String cycle_expanded = null;
		
		
		String[] generated_repeats_info = new String[4];
		
			
		for(int i = min_motif_size; i < max_motif_size+ 1; i++){
			alphabet_repeat = product(alphabet,i,string);
			for(String combination : alphabet_repeat){
				String repeat_revcomp = rev_comp(combination);
				String expanded = expand_repeat(combination, max_motif_size);
				
				if(expanded_set.contains(expanded)){
					continue;
				}else {
					
					List<String> repeat_cycles = get_cycles(combination);
					
					for(String cycle:repeat_cycles){
						strand = "+";
						cycle_expanded = expand_repeat(cycle, max_motif_size);
						expanded_set.add(cycle_expanded);
						if(!repeat_set.contains(cycle)){
							repeat_set.add(cycle);
							generated_repeats_info[0] = cycle;
							generated_repeats_info[1] = combination;
							generated_repeats_info[2] = Integer.toString(cycle.length());
							generated_repeats_info[3] = strand;
							
							generated_repeats.add(cycle + "\t" + combination + "\t" + cycle.length() + "\t" + strand);
						}
					}
					if(repeat_revcomp.equals(combination)){
						continue;
					}
					repeat_cycles = get_cycles(repeat_revcomp);
					for(String cycle:repeat_cycles){
						strand = "-";
						cycle_expanded = expand_repeat(cycle, max_motif_size);
						expanded_set.add(cycle_expanded);
						if(!repeat_set.contains(cycle)){
							repeat_set.add(cycle);
							generated_repeats_info[0] = cycle;
							generated_repeats_info[1] = combination;
							generated_repeats_info[2] = Integer.toString(cycle.length());
							generated_repeats_info[3] = strand;
							generated_repeats.add(cycle + "\t" + combination + "\t" + cycle.length() + "\t" + strand);
						}
					}
				}
			}
		}
		return generated_repeats;
	}
	
	  
}
