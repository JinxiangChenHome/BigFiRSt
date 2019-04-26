package nwafu.cie.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


import nwafu.cie.common.PerfOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class Analyse_perf {
	
	public void analyse(PerfOptions perfOptions, Configuration conf){
		
		String projectPath = this.getClass().getResource("/html/template.html").getPath();
		
		String repeatsOutFile = perfOptions.getOutputPath();
		String outHTML = perfOptions.getAnalyse();
		Map<String, Object> defaultInfoMap = new HashMap<String, Object>();
		Map<String, Object> infoMap = new HashMap<String, Object>();
		
		List<Object> seqInfoList = new ArrayList<Object>();
		
		defaultInfoMap.put("info", infoMap);
		infoMap.put("seqInfo", seqInfoList);
		
		List<String> repFreqByClassList = new ArrayList<String>();
		List<String> repBasesByClassList = new ArrayList<String>();
		
		Map<String, Object> plotDataMap = new HashMap<String, Object>();
		Map<String, Object> replenMap = new HashMap<String, Object>();
		Map<String, Object> repunitMap = new HashMap<String, Object>();
		
		plotDataMap.put("replen", replenMap);
		plotDataMap.put("repunit", repunitMap);
		
		Map<String, Object> plotInfoMap = new HashMap<String, Object>();
		Map<String, Object> lenMap = new HashMap<String, Object>();
		Map<String, Object> unitMap = new HashMap<String, Object>();
		
		plotInfoMap.put("len", lenMap);
		plotInfoMap.put("unit", unitMap);
		
		List<Object> longestLengthsList = new ArrayList<Object>();
		
		
		for (int i = 0; i < 100; i++) {
			List<Object> subLongestLengthsList = new ArrayList<Object>();
			subLongestLengthsList.add("seq");
			subLongestLengthsList.add("start");
			subLongestLengthsList.add("stop");
			subLongestLengthsList.add("repClass");
			subLongestLengthsList.add(0);
			subLongestLengthsList.add("+");
			subLongestLengthsList.add(0);
			subLongestLengthsList.add("actualrep");
			longestLengthsList.add(subLongestLengthsList);
		}
		
		List<Object> mostUnitsList = new ArrayList<Object>();
		
		
		for (int i = 0; i < 100; i++) {
			List<Object> subMostUnitsList = new ArrayList<Object>();
			subMostUnitsList.add("seq");
			subMostUnitsList.add("start");
			subMostUnitsList.add("stop");
			subMostUnitsList.add("repClass");
			subMostUnitsList.add(0);
			subMostUnitsList.add("+");
			subMostUnitsList.add(0);
			subMostUnitsList.add("actualrep");
			mostUnitsList.add(subMostUnitsList);
		}
		
		double minLength = Double.POSITIVE_INFINITY;
		double minUnits = Double.POSITIVE_INFINITY;
		
		Set<Integer> repLengthSet = new HashSet<Integer>();
		Set<Integer> unitsSet = new HashSet<Integer>();
		
		//Process the results of the PERF output
		Map<String, Integer> staDataMap2 = processPERFRes(perfOptions, conf, repeatsOutFile, minLength, minUnits, longestLengthsList, mostUnitsList, plotDataMap, replenMap, repunitMap, plotInfoMap, lenMap, unitMap, repLengthSet, unitsSet);
		
		minLength = staDataMap2.get("minLength");
		minUnits = staDataMap2.get("minUnits");
		
		String dataSelectedView1 = "{";
		String myData1 = "[";
		String dataSelectedView2 = "{";
		String myData2 = "[";
		String line1Dataset = "";
		String dataSelectedView3 = "{";
		String plotLines3 = "";
		String line2Dataset = "";
		String dataSelectedView4 = "{";
		String plotLines4 = "";
		
		String plotTable1 = "";
		String plotTable2 = "";
		
		Map<String, Object> plotDataReplenMap2 = (Map<String, Object>) plotDataMap.get("replen");
		int numAA=plotDataReplenMap2.keySet().size();
		int ii = 0;
		for(String rep : plotDataReplenMap2.keySet()){
			List<Integer> frepsList = transformValueList((Map<Integer, Integer>) plotDataReplenMap2.get(rep));
			repFreqByClassList.add("name:'" + rep + "',value:'" + sumList(frepsList) + "'");
			repBasesByClassList.add("name:'" + rep + "',value:'" + sumList(mapList(frepsList, minLength)) + "'");
			if(ii < numAA-1 && ii >8){
				dataSelectedView1 += "'" + rep + "':false,";
				if(ii >10)
					dataSelectedView2 += "'" + rep + "':false,";
			} else if(ii == numAA-1){
				dataSelectedView1 += "'" + rep + "':false";
				dataSelectedView2 += "'" + rep + "':false";
			}
			
			
			List<Integer> lenfreqsList = new ArrayList<Integer>();
			List<Integer> unitfreqsList = new ArrayList<Integer>();
			
			List<Integer> plotDataReplenRep = transformKeyList((Map<Integer, Integer>) (((Map<String, Object>) plotDataMap.get("replen"))).get(rep));
			quickSortForList(plotDataReplenRep, 0,plotDataReplenRep.size() - 1);
			List<Integer> lengthsList = plotDataReplenRep;
			
			List<Integer> plotDataRepunitRep = transformKeyList((Map<Integer, Integer>) (((Map<String, Object>) plotDataMap.get("repunit"))).get(rep));
			
			
			quickSortForList(plotDataRepunitRep, 0,plotDataRepunitRep.size() - 1);
			List<Integer> unitsList = plotDataRepunitRep;
			
			for (int i = (int) minLength; i < lengthsList.get(lengthsList.size()-1) + 1; i++) {
				if(((Map<Integer, Integer>) (((Map<String, Object>) plotDataMap.get("replen"))).get(rep)).get(i)!=null){
					lenfreqsList.add(((Map<Integer, Integer>) (((Map<String, Object>) plotDataMap.get("replen"))).get(rep)).get(i));
				}else {
					lenfreqsList.add(0);
				}
			}
			
			for (int i = (int) minUnits; i < unitsList.get(unitsList.size()-1) + 1; i++) {
				if(((Map<Integer, Integer>) (((Map<String, Object>) plotDataMap.get("repunit"))).get(rep)).get(i)!=null){
					unitfreqsList.add(((Map<Integer, Integer>) (((Map<String, Object>) plotDataMap.get("repunit"))).get(rep)).get(i));
				}else {
					unitfreqsList.add(0);
				}
			}
			
			lenMap.put(rep, lenfreqsList);
			unitMap.put(rep, unitfreqsList);
			
			ii ++;
		}
		
		
		infoMap.put("plotInfo", plotInfoMap);
		infoMap.put("numRepClass", repFreqByClassList.size());
		infoMap.put("repFreqByClass", repFreqByClassList);
		infoMap.put("repBasesByClass", repBasesByClassList);
		infoMap.put("minLength", (int) minLength);
		infoMap.put("minUnits", (int) minUnits);
		
		
		
		for(Object a : longestLengthsList){
			List<Object> bList = (List<Object>) a;
			plotTable1 += "<tr><td>" + bList.get(0)+ "</td><td>" + bList.get(1)+ "</td><td>" + bList.get(2)+ "</td><td>" + bList.get(3)+ "</td><td>" + bList.get(4)+ "</td><td>" + bList.get(5)+ "</td><td>" + bList.get(6)+ "</td><td>" + bList.get(7)+ "</td></tr>";
		}
		
		
		for(Object a : mostUnitsList){
			List<Object> bList = (List<Object>) a;
			plotTable2 += "<tr><td>" + bList.get(0)+ "</td><td>" + bList.get(1)+ "</td><td>" + bList.get(2)+ "</td><td>" + bList.get(3)+ "</td><td>" + bList.get(4)+ "</td><td>" + bList.get(5)+ "</td><td>" + bList.get(6)+ "</td><td>" + bList.get(7)+ "</td></tr>";
		}
		
		
		int numRepFreqByClassList = repFreqByClassList.size();
		int i=0;
		for (String str:repFreqByClassList) {
			if(i<numRepFreqByClassList-1){
				myData1 += "{"+str+"},";
			}else if(i==numRepFreqByClassList-1){
				myData1 += "{"+str+"}";
			}
			i++;
		}
		
		int numRepBasesByClassList = repBasesByClassList.size();
		int j=0;
		for (String str:repBasesByClassList) {
			if(j<numRepBasesByClassList-1){
				myData2 += "{"+str+"},";
			}else if(j==numRepBasesByClassList-1){
				myData2 += "{"+str+"}";
			}
			j++;
		}
		
		int plotMinRepLength = 0;
		int plotMaxRepLength = 0;
		for(Integer plotA:repLengthSet){
			if(plotA < plotMinRepLength){
				plotMinRepLength = plotA;
			}
			if(plotA > plotMaxRepLength){
				plotMaxRepLength = plotA;
			}
		}
		
		line1Dataset += "[['Sequence'";
		int k = plotMinRepLength;
		while (k <= plotMaxRepLength) {
				line1Dataset += ", '"+String.valueOf(k) + "'";
				k++;
		}
		line1Dataset += "]";
		for(String replensetObject : replenMap.keySet()){
			Map<Integer, Integer> replenRepClass = (Map<Integer, Integer>) replenMap.get(replensetObject);
			int aa = plotMinRepLength;
			line1Dataset += ", ['"+replensetObject + "'";
			while (aa <= plotMaxRepLength) {
				if(replenRepClass.get(aa)!=null){
					line1Dataset += ", "+ replenRepClass.get(aa);
				}else {
					line1Dataset += ", "+ "0";
				}
				aa++;
			}
			line1Dataset += "]";
		}
		
		
		int kk = 0 ;
		while(kk < replenMap.size()){
			if(kk != replenMap.size()-1){
				plotLines3 += "{type: 'line', smooth: true, seriesLayoutBy: 'row'},";
			}
			else {
				plotLines3 += "{type: 'line', smooth: true, seriesLayoutBy: 'row'}";
			}
			kk++;
		}
		
		int plotMinUnits = 0;
		int plotMaxUnits = 0;
		for(Integer plotB:unitsSet){
			if(plotB < plotMinUnits){
				plotMinUnits = plotB;
			}
			if(plotB > plotMaxUnits){
				plotMaxUnits = plotB;
			}
		}
		line2Dataset += "[['Sequence'";
		int k2 = plotMinUnits;
		while (k2 <= plotMaxUnits) {
				line2Dataset += ", '"+String.valueOf(k2) + "'";
				k2++;
		}
		line2Dataset += "]";
		for(String replensetObject : repunitMap.keySet()){
			Map<Integer, Integer> replenRepClass = (Map<Integer, Integer>) repunitMap.get(replensetObject);
			int aa = plotMinUnits;
			line2Dataset += ", ['"+replensetObject + "'";
			while (aa <= plotMaxUnits) {
				if(replenRepClass.get(aa)!=null){
					line2Dataset += ", "+ replenRepClass.get(aa);
				}else {
					line2Dataset += ", "+ "0";
				}
				aa++;
			}
			line2Dataset += "]";
		}
		
		
		dataSelectedView1 += "};";
		myData1 += "];";
		dataSelectedView2 += "};";
		myData2 += "];";
		line1Dataset += "];";
		dataSelectedView3 = dataSelectedView1;
		line2Dataset += "];";
		dataSelectedView4 = dataSelectedView2;
		plotLines4 = plotLines3;
		
		writeHTML(projectPath, outHTML, dataSelectedView1, myData1, dataSelectedView2, myData2, line1Dataset, dataSelectedView3, line2Dataset, dataSelectedView4, plotLines3, plotLines4, plotTable1, plotTable2);
		
	}
	
	public void writeHTML(String input, String outHTML, String dataSelectedView1, String myData1, String dataSelectedView2, String myData2, String line1Dataset, String dataSelectedView3, String line2Dataset, String dataSelectedView4, String plotLines3, String plotLines4, String plotTable1, String plotTable2){
		
		try {
			String encoding = "UTF-8";
			File file = new File(input);
			if (file.isFile() && file.exists()) {
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
				BufferedReader bufferedReader = new BufferedReader(read);
				
				FileWriter fw = new FileWriter(outHTML);
				BufferedWriter bw = new BufferedWriter(fw);
				
				
				String lineTxt = null;
				int i = 0;
				int start_index = 0;
				int stop_index = 0;
				
				while ((lineTxt = bufferedReader.readLine()) != null) {
					
					if(i>2900 && i<3390){
						start_index = lineTxt.indexOf("^^");
						stop_index = lineTxt.indexOf("$$");
						if(start_index != -1 && stop_index != -1 ){
							if(lineTxt.substring(start_index + 2, stop_index).equals("dataSelectedView1")){
								bw.write("var dataSelectedView1 = " + dataSelectedView1);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("myData1")){
								bw.write("var myData1 = " + myData1);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("dataSelectedView2")){
								bw.write("var dataSelectedView2 = " + dataSelectedView2);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("dataSelectedView3")){
								bw.write("var dataSelectedView3 = " + dataSelectedView3);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("dataSelectedView4")){
								bw.write("var dataSelectedView4 = " + dataSelectedView4);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("myData2")){
								bw.write("var myData2 = " + myData2);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("line1Dataset")){
								bw.write("var line1Dataset = " + line1Dataset);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("line2Dataset")){
								bw.write("var line2Dataset = " + line2Dataset);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("plotLines3")){
								bw.write(plotLines3);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("plotLines4")){
								bw.write(plotLines4);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("plotTable1")){
								bw.write(plotTable1);
							}else if(lineTxt.substring(start_index + 2, stop_index).equals("plotTable2")){
								bw.write(plotTable2);
							}
							
							bw.newLine();
						}else {
							bw.write(lineTxt);
							bw.newLine();
						}
						
					}else {
						bw.write(lineTxt);
						bw.newLine();
					}
					i++;	
				}
				bw.close();
				read.close();
			} else {
				System.out.println("The specified file could not be found");
			}
		} catch (Exception e) {
			System.out.println("Error reading file contents");
			e.printStackTrace();
		}
		
		
		
	}
	
	public List<Integer> transformValueList(Map<Integer, Integer> inputMap){
		List<Integer> list = new ArrayList<Integer>();
		for(int a:inputMap.values()){
			list.add(a);
		}
		return list;
	}
	
	public List<Integer> transformKeyList(Map<Integer, Integer> inputMap){
		List<Integer> list = new ArrayList<Integer>();
		for(int a:inputMap.keySet()){
			list.add(a);
		}
		return list;
	}
	
	public void quickSortForList(List<Integer> list, int low, int high){
		int start = low;
		int end = high;
		int key = list.get(start);
		
		while(end > start){
			// Specified comparison from back to front
			while( end > start && list.get(end) >= key){
				end--;
			}
			if(list.get(end) <= key){
				int tmp = list.get(end);
				list.set(end, list.get(start));
				list.set(start, tmp);
			}
			
			// Specified comparison from front to back
			while( end > start && list.get(start) <= key){
				start++;
			}
			if(list.get(start) >= key){
				int tmp = list.get(start);
				list.set(start, list.get(end));
				list.set(end, tmp);
			}
		}
		
		//recursion
		if(start > low)
			quickSortForList(list, low, start-1);
		if(end < high)
			quickSortForList(list, end + 1, high);
	}
	

	
	//map the list collection
	public List<Integer> mapList(List<Integer> list, double minLength){
		List<Integer> mapList = new ArrayList<Integer>();
		int minLengthInt = (int) minLength;
		int num;
		int i;
		for(int j = 0; j < list.size(); j++){
			num = list.get(j);
			for(i = 0; i < list.size(); i++){
				if(list.get(i) == num){
					break;
				}
			}
			mapList.add(num * (minLengthInt + i));
		}
		return mapList;
	}
	
	//sum the contents of the list collection
	public int sumList(List<Integer> list){
		int sum = 0;
		for(int num : list){
			sum += num;
		}
		return sum;
	}
	
	//Process the results of the PERF output
	public Map<String, Integer> processPERFRes(PerfOptions perfOptions, Configuration conf, String filePath, double minLength, double minUnits, List<Object> longestLengthsList, List<Object> mostUnitsList, Map<String, Object> plotDataMap, Map<String, Object> replenMap, Map<String, Object> repunitMap, Map<String, Object> plotInfoMap, Map<String, Object> lenMap, Map<String, Object> unitMap, Set<Integer> repLengthSet, Set<Integer> unitsSet) {
		Map<String, Integer> staDataMap = new HashMap<String, Integer>();
		try {
			FileSystem fs = FileSystem.get(new URI("hdfs://"+perfOptions.getMasterHostname()+":"+perfOptions.getClusterPort()),conf);
			
			FileStatus[] status = fs.listStatus(new Path(filePath)); 
			
			for (FileStatus file : status) {  
			    
				if (!file.getPath().getName().startsWith("part")) {  
			              continue;  
			          }  
				System.out.println("Processing: "+file.getPath().getName());
			    FSDataInputStream hdfsInStream = fs.open(file.getPath());
			    String encoding = "UTF-8";
			    InputStreamReader read = new InputStreamReader(hdfsInStream, encoding);
			    
			    BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				String[] fields = null;
				
				String seq = null;
				String start = null;
				String end = null;
				String repClass = null;
				String repLength = null;
				String repOri = null;
				String repUnit = null;
				String actualRepeat = null;
				
				int startInt;
				int endInt;
				int repLengthInt;
				int repUnitInt;
				
				while ((lineTxt = bufferedReader.readLine()) != null) {
					lineTxt = lineTxt.trim();
					fields = lineTxt.split("  ");
					
					seq = fields[0];
					start = fields[1];
					end = fields[2];
					repClass = fields[3];
					repLength = fields[4];
					repOri = fields[5];
					repUnit = fields[6];
					actualRepeat = fields[7];
					
					startInt = Integer.parseInt(start);
					endInt = Integer.parseInt(end);
					repLengthInt = Integer.parseInt(repLength);
					repUnitInt = Integer.parseInt(repUnit);
					
					repLengthSet.add(repLengthInt);
					unitsSet.add(repUnitInt);
					
					
					if(minUnits > repUnitInt){
						minUnits = (double) repUnitInt;
					}
					
					if(minLength > repLengthInt){
						minLength = (double) repLengthInt;
					}
					
					List<Object> firstLongestLengthsList = ((ArrayList<Object>) longestLengthsList.get(99));
					int columnLongestLengthsList = 4;
					
					if((int) (firstLongestLengthsList.get(columnLongestLengthsList)) < repLengthInt ){
						((ArrayList<Object>) longestLengthsList.get(99)).set(0, seq);
						((ArrayList<Object>) longestLengthsList.get(99)).set(1, startInt);
						((ArrayList<Object>) longestLengthsList.get(99)).set(2, endInt);
						((ArrayList<Object>) longestLengthsList.get(99)).set(3, repClass);
						((ArrayList<Object>) longestLengthsList.get(99)).set(4, repLengthInt);
						((ArrayList<Object>) longestLengthsList.get(99)).set(5, repOri);
						((ArrayList<Object>) longestLengthsList.get(99)).set(6, repUnitInt);
						((ArrayList<Object>) longestLengthsList.get(99)).set(7, actualRepeat);
						
					}else if((int) (firstLongestLengthsList.get(columnLongestLengthsList)) == repLengthInt ){
						if(repClass.compareTo(firstLongestLengthsList.get(3).toString()) < 0){
							((ArrayList<Object>) longestLengthsList.get(99)).set(0, seq);
							((ArrayList<Object>) longestLengthsList.get(99)).set(1, startInt);
							((ArrayList<Object>) longestLengthsList.get(99)).set(2, endInt);
							((ArrayList<Object>) longestLengthsList.get(99)).set(3, repClass);
							((ArrayList<Object>) longestLengthsList.get(99)).set(4, repLengthInt);
							((ArrayList<Object>) longestLengthsList.get(99)).set(5, repOri);
							((ArrayList<Object>) longestLengthsList.get(99)).set(6, repUnitInt);
							((ArrayList<Object>) longestLengthsList.get(99)).set(7, actualRepeat);
						}
					}
					
					sortColumnArrayList(longestLengthsList, columnLongestLengthsList);
					
					List<Object> firstMostUnitsList = ((ArrayList<Object>) mostUnitsList.get(99));
					int columnMostUnitsList = 6;
					
					if((int) (firstMostUnitsList.get(columnMostUnitsList)) < repUnitInt ){
						((ArrayList<Object>) mostUnitsList.get(99)).set(0, seq);
						((ArrayList<Object>) mostUnitsList.get(99)).set(1, startInt);
						((ArrayList<Object>) mostUnitsList.get(99)).set(2, endInt);
						((ArrayList<Object>) mostUnitsList.get(99)).set(3, repClass);
						((ArrayList<Object>) mostUnitsList.get(99)).set(4, repLengthInt);
						((ArrayList<Object>) mostUnitsList.get(99)).set(5, repOri);
						((ArrayList<Object>) mostUnitsList.get(99)).set(6, repUnitInt);
						((ArrayList<Object>) mostUnitsList.get(99)).set(7, actualRepeat);
						
					}else if((int) (firstMostUnitsList.get(columnMostUnitsList)) == repUnitInt ){
						if(repClass.compareTo(firstMostUnitsList.get(3).toString()) < 0){
							((ArrayList<Object>) mostUnitsList.get(99)).set(0, seq);
							((ArrayList<Object>) mostUnitsList.get(99)).set(1, startInt);
							((ArrayList<Object>) mostUnitsList.get(99)).set(2, endInt);
							((ArrayList<Object>) mostUnitsList.get(99)).set(3, repClass);
							((ArrayList<Object>) mostUnitsList.get(99)).set(4, repLengthInt);
							((ArrayList<Object>) mostUnitsList.get(99)).set(5, repOri);
							((ArrayList<Object>) mostUnitsList.get(99)).set(6, repUnitInt);
							((ArrayList<Object>) mostUnitsList.get(99)).set(7, actualRepeat);
						}
					}
					sortColumnArrayList(mostUnitsList, columnMostUnitsList);
					
					if(!replenMap.containsKey(repClass)){
						Map<Integer, Integer> replenRepClassMap = new HashMap<Integer, Integer>();
						replenRepClassMap.put(repLengthInt, 1);
						replenMap.put(repClass, replenRepClassMap);
						
						
						Map<Integer, Integer> repunitRepClassMap = new HashMap<Integer, Integer>();
						repunitRepClassMap.put(repUnitInt, 1);
						repunitMap.put(repClass, repunitRepClassMap);
						
						
					}else if (replenMap.containsKey(repClass)) {
						if(!((Map<Integer, Integer>) replenMap.get(repClass)).containsKey(repLengthInt) ){
							((Map<Integer, Integer>) replenMap.get(repClass)).put(repLengthInt, 1);
						}else if (((Map<Integer, Integer>) replenMap.get(repClass)).containsKey(repLengthInt)) {
							((Map<Integer, Integer>) replenMap.get(repClass)).put(repLengthInt, (int) ((Map<Integer, Integer>) replenMap.get(repClass)).get(repLengthInt) + 1);
						}
						
						if(!((Map<Integer, Integer>) repunitMap.get(repClass)).containsKey(repUnitInt) ){
							((Map<Integer, Integer>) repunitMap.get(repClass)).put(repUnitInt, 1);
						}else if (((Map<Integer, Integer>) repunitMap.get(repClass)).containsKey(repUnitInt)) {
							((Map<Integer, Integer>) repunitMap.get(repClass)).put(repUnitInt, (int) ((Map<Integer, Integer>) repunitMap.get(repClass)).get(repUnitInt) + 1);
						}
						
					}
					
				}
				read.close();
			} 
		} catch (Exception e) {
			System.out.println("Error reading file contents");
			e.printStackTrace();
		}
		
		staDataMap.put("minLength", (int) minLength);
		staDataMap.put("minUnits", (int) minUnits);
		return staDataMap;
		
	}
	
	// Sort the specific column of the ArrayList
	public void sortColumnArrayList(List<Object> readySortedList, int column){
		int start = 0;
		int end = 99;
		quickSort(readySortedList, start, end, column);
		
	}
	
	public void quickSort(List<Object> readySortedList, int low, int high, int column){
		int start = low;
		int end = high;
		int key = (int) ((List<Object>) readySortedList.get(low)).get(column);
		
		while(end > start){
			// Specified comparison from back to front
			while( end > start && ((int) ((List<Object>) readySortedList.get(end)).get(column)) <= key){
				end--;
			}
			if(((int) ((List<Object>) readySortedList.get(end)).get(column)) >= key){
				int tmp = (int) ((List<Object>) readySortedList.get(end)).get(column);
				List<Object> tmp2 = (List<Object>) readySortedList.get(end);
				readySortedList.set(end, (List<Object>) readySortedList.get(start));
				readySortedList.set(start, tmp2);
				
			}
			
			// Specified comparison from front to back
			while( end > start && ((int) ((List<Object>) readySortedList.get(start)).get(column)) >= key){
				start++;
			}
			if(((int) ((List<Object>) readySortedList.get(start)).get(column)) <= key){
				int tmp = (int) ((List<Object>) readySortedList.get(start)).get(column);
				List<Object> tmp2 = (List<Object>) readySortedList.get(start);
				readySortedList.set(start, (List<Object>) readySortedList.get(end));
				readySortedList.set(end, tmp2);
				
			}
		}
		
		//recursion
		if(start > low)
			quickSort(readySortedList, low, start-1, column);
		if(end < high)
			quickSort(readySortedList, end + 1, high, column);
	}
	
	
	
}
