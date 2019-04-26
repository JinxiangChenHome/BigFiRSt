package nwafu.cie.common;

import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class PerfOptions {
	
	private String inputPath = "";
	
	private String outputPath = "";
	
	private String analyse = "";

	private String min_length = "";
	
	private String min_units = "";
	
	private List<String> repeats = null;
	
	private String repeats_path = "";

	private String min_motif_size = "";
	
	private String max_motif_size = "";
	
	private String min_seq_length = "";

	private String max_seq_length = "";
	
	private String filter_seq_ids = "";
	
	private String target_seq_ids = "";
	
	private String version = "";
	
	private String numReduceTasks = "";
	
	private String masterHostname = "";
	
	private String clusterPort = "";
	
	private String flank_len = "";
	
	private Options options = null;

	private String[] commandLine;

	public PerfOptions() {
		this.options = new Options();
	}

	public PerfOptions(String[] args) {
		this.commandLine = args;
		this.options = new Options();
		initOptions();
		parseParameters(args);
	}
	
	public void initOptions() {
		Option in = new Option("i", "input", true, "Input file in FASTA format");
		this.options.addOption(in);

		Option out = new Option("o", "output", true, "Output file name. Default: Input file name + _perf.tsv");
		this.options.addOption(out);

		Option analyse = new Option("a", "analyse", true, "Generate a summary HTML report.");
		this.options.addOption(analyse);
		
		Option min_length = new Option("l", "min-length", true, "Minimum length cutoff of repeat");
		this.options.addOption(min_length);
		
		Option min_units = new Option("u", "min-units", true, "Minimum number of repeating units to be considered. Can be an integer or a file specifying cutoffs for different motif sizes.");
		this.options.addOption(min_units);
		
		Option repeats = new Option("r", "repeats", true, "File with list of repeats (Not allowed with -m and/or -M)");
		this.options.addOption(repeats);
		
		Option min_motif_size = new Option("m", "min-motif-size", true, "Minimum size of a repeat motif in bp (Not allowed with -rep)");
		this.options.addOption(min_motif_size);
		
		Option max_motif_size = new Option("M", "max-motif-size", true, "Maximum size of a repeat motif in bp (Not allowed with -rep)");
		this.options.addOption(max_motif_size);
		
		Option min_seq_length = new Option("s", "min-seq-length", true, "Minimum size of sequence length for consideration (in bp)");
		this.options.addOption(min_seq_length);
		
		Option max_seq_length = new Option("S", "max-seq-length", true, "Maximum size of sequence length for consideration (in bp)");
		this.options.addOption(max_seq_length);
		
		Option filter_seq_ids = new Option("f", "filter-seq-ids", true, "");
		this.options.addOption(filter_seq_ids);
		
		Option target_seq_ids = new Option("F", "target-seq-ids", true, "");
		this.options.addOption(target_seq_ids);

		Option version = new Option("v", "version", true, "");
		this.options.addOption(version);
		
		Option numReduceTasks = new Option("n", "numReduceTasks", true, "Number of Reduce");
		this.options.addOption(numReduceTasks);
		
		Option masterHostname = new Option("mH", "masterHostname", true, "The host name of the master node in the hadoop cluster");
		this.options.addOption(masterHostname);
		
		Option clusterPort = new Option("cP", "clusterPort", true, "Rpc port of hdfs in hadoop cluster");
		this.options.addOption(clusterPort);
		
		Option flank_len = new Option("fl", "flank_len", true, "Length of terminal flanking sequences");
		this.options.addOption(flank_len);
	}
	
	public void parseParameters(String[] args) {

		CommandLineParser parser = new BasicParser();
		CommandLine cmd;

		try {
			cmd = parser.parse(this.options, args);

			if (cmd.hasOption('i') || cmd.hasOption("input")) {
				inputPath = cmd.getOptionValue("input");
			}

			if (cmd.hasOption('o') || cmd.hasOption("output")) {
				outputPath = cmd.getOptionValue("output");
			}
			
			if (cmd.hasOption('a') || cmd.hasOption("analyse")) {
				analyse = cmd.getOptionValue("analyse");
			}
			
			if (cmd.hasOption('l') || cmd.hasOption("min-length")) {
				min_length = cmd.getOptionValue("min-length");
			}
			
			if (cmd.hasOption('u') || cmd.hasOption("min-units")) {
				min_units = cmd.getOptionValue("min-units");
			}
			
			if (cmd.hasOption('r') || cmd.hasOption("repeats")) {
				repeats_path = cmd.getOptionValue("repeats");
			}
			
			if (cmd.hasOption('m') || cmd.hasOption("min-motif-size")) {
				min_motif_size = cmd.getOptionValue("min-motif-size");
			}
			
			if (cmd.hasOption('M') || cmd.hasOption("max-motif-size")) {
				max_motif_size = cmd.getOptionValue("max-motif-size");
			}
			
			if (cmd.hasOption('s') || cmd.hasOption("min-seq-length")) {
				min_seq_length = cmd.getOptionValue("min-seq-length");
			}
			
			if (cmd.hasOption('S') || cmd.hasOption("max-seq-length")) {
				max_seq_length = cmd.getOptionValue("max-seq-length");
			}
			if (cmd.hasOption('f') || cmd.hasOption("filter-seq-ids")) {
				filter_seq_ids = cmd.getOptionValue("filter-seq-ids");
			}
			
			if (cmd.hasOption('F') || cmd.hasOption("target-seq-ids")) {
				target_seq_ids = cmd.getOptionValue("target-seq-ids");
			}
			
			if (cmd.hasOption('v') || cmd.hasOption("version")) {
				version = cmd.getOptionValue("version");
			}
			
			if (cmd.hasOption('n') || cmd.hasOption("numReduceTasks")) {
				numReduceTasks = cmd.getOptionValue("numReduceTasks");
			}
			
			if (cmd.hasOption("mH") || cmd.hasOption("masterHostname")) {
				masterHostname = cmd.getOptionValue("masterHostname");
			}
			
			if (cmd.hasOption("cP") || cmd.hasOption("clusterPort")) {
				clusterPort = cmd.getOptionValue("clusterPort");
			}
			if (cmd.hasOption("fl") || cmd.hasOption("flank_len")) {
				flank_len = cmd.getOptionValue("flank_len");
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
			System.exit(1);

		}
	}
	
	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getAnalyse() {
		return analyse;
	}

	public void setAnalyse(String analyse) {
		this.analyse = analyse;
	}

	public String getMin_length() {
		return min_length;
	}

	public void setMin_length(String min_length) {
		this.min_length = min_length;
	}

	public String getMin_units() {
		return min_units;
	}

	public void setMin_units(String min_units) {
		this.min_units = min_units;
	}

	public List<String> getRepeats() {
		return repeats;
	}

	public void setRepeats(List<String> repeats) {
		this.repeats = repeats;
	}

	public String getRepeats_path() {
		return repeats_path;
	}

	public void setRepeats_path(String repeats_path) {
		this.repeats_path = repeats_path;
	}

	public String getMin_motif_size() {
		return min_motif_size;
	}

	public void setMin_motif_size(String min_motif_size) {
		this.min_motif_size = min_motif_size;
	}

	public String getMax_motif_size() {
		return max_motif_size;
	}

	public void setMax_motif_size(String max_motif_size) {
		this.max_motif_size = max_motif_size;
	}

	public String getMin_seq_length() {
		return min_seq_length;
	}

	public void setMin_seq_length(String min_seq_length) {
		this.min_seq_length = min_seq_length;
	}

	public String getMax_seq_length() {
		return max_seq_length;
	}

	public void setMax_seq_length(String max_seq_length) {
		this.max_seq_length = max_seq_length;
	}

	public String getFilter_seq_ids() {
		return filter_seq_ids;
	}

	public void setFilter_seq_ids(String filter_seq_ids) {
		this.filter_seq_ids = filter_seq_ids;
	}

	public String getTarget_seq_ids() {
		return target_seq_ids;
	}

	public void setTarget_seq_ids(String target_seq_ids) {
		this.target_seq_ids = target_seq_ids;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getNumReduceTasks() {
		return numReduceTasks;
	}

	public void setNumReduceTasks(String numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}

	public String getMasterHostname() {
		return masterHostname;
	}

	public void setMasterHostname(String masterHostname) {
		this.masterHostname = masterHostname;
	}

	public String getClusterPort() {
		return clusterPort;
	}

	public void setClusterPort(String clusterPort) {
		this.clusterPort = clusterPort;
	}

	public String getFlank_len() {
		return flank_len;
	}

	public void setFlank_len(String flank_len) {
		this.flank_len = flank_len;
	}

	@Override
	public String toString() {
		StringBuffer buff = new StringBuffer("");
		for (int i = 0; i < commandLine.length;) {
			String arg = commandLine[i].trim();
			if (arg.equals("-i") || arg.equals("-o")) {
				i = i + 2;
			} else {
				i++;
				buff.append(arg + " ");
			}
		}
		return buff.toString();
	}
	
}
