package com.nwafu.cjx.common;

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
public class FlashOptions {
	
	private String hinputPath = "";
	
	private String houtputPath = "";
	
	private String min_overlap = "10";

	private String max_overlap = "65";
	
	private String max_mismatch_density = "0.25";

	private String allow_outies = "";
	
	private String phred_offset = "33";
	
	private String read_len = "";

	private String fragment_len = "";
	
	private String fragment_len_stddev = "";
	
	private String interleaved = "";
	
	private String tab_delimited_input = "";
	
	private String tab_delimited_output = "";
	
	private String output_prefix = "out";
	
	private String output_directory = "";
	
	private String to_stdout = "";
	
	private String compress = "";
	
	private String threads = "";
	
	private String quiet = "";
	
	private String help = "";
	
	private String version = "";
	
	private String cap_mismatch_quals = "";
	
	private String interleaved_input = "";
	
	private String interleaved_output = "";
	
	private String compress_prog = "";
	
	private String compress_prog_args = "";
	
	private String suffix = "";
	
	private String output_suffix = "";
	
	private String numReduceTasks = "";
	
	private String masterHostname = "localhost";
	
	private String clusterPort = "8020";
	
	private Options options = null;

	private String[] commandLine;

	public FlashOptions() {
		this.options = new Options();
	}

	public FlashOptions(String[] args) {
		this.commandLine = args;
		this.options = new Options();
		initOptions();
		parseParameters(args);
	}
	
	public void initOptions() {
		Option hin = new Option("hi", "hinput", true, "Input file in FASTA format");
		this.options.addOption(hin);

		Option hout = new Option("ho", "houtput", true, "Output file name. Default: Input file name + _perf.tsv");
		this.options.addOption(hout);

		Option min_overlap = new Option("m", "min-overlap", true, "The minimum required overlap length between two reads to provide a confident overlap.  Default: 10bp.");
		this.options.addOption(min_overlap);
		
		Option max_overlap = new Option("M", "max-overlap", true, "Minimum length cutoff of repeat");
		this.options.addOption(max_overlap);
		
		Option max_mismatch_density = new Option("x", "max-mismatch-density", true, "Minimum number of repeating units to be considered. Can be an integer or a file specifying cutoffs for different motif sizes.");
		this.options.addOption(max_mismatch_density);
		
		Option allow_outies = new Option("O", "allow-outies", true, "File with list of repeats (Not allowed with -m and/or -M)");
		this.options.addOption(allow_outies);
		
		Option phred_offset = new Option("p", "phred-offset", true, "Minimum size of a repeat motif in bp (Not allowed with -rep)");
		this.options.addOption(phred_offset);
		
		Option read_len = new Option("r", "read-len", true, "Maximum size of a repeat motif in bp (Not allowed with -rep)");
		this.options.addOption(read_len);
		
		Option fragment_len = new Option("f", "fragment-len", true, "Minimum size of sequence length for consideration (in bp)");
		this.options.addOption(fragment_len);
		
		Option fragment_len_stddev = new Option("s", "fragment-len-stddev", true, "Maximum size of sequence length for consideration (in bp)");
		this.options.addOption(fragment_len_stddev);
		
		Option interleaved = new Option("I", "interleaved", true, "");
		this.options.addOption(interleaved);
		
		Option tab_delimited_input = new Option("Ti", "tab-delimited-input", true, "");
		this.options.addOption(tab_delimited_input);
		
		Option tab_delimited_output = new Option("To", "tab-delimited-output", true, "");
		this.options.addOption(tab_delimited_output);
		
		Option output_prefix = new Option("o", "output-prefix", true, "");
		this.options.addOption(output_prefix);
		
		Option output_directory = new Option("d", "output-directory", true, "");
		this.options.addOption(output_directory);
		
		Option to_stdout = new Option("c", "to-stdout", true, "");
		this.options.addOption(to_stdout);
		
		Option compress = new Option("z", "compress", true, "");
		this.options.addOption(compress);
		
		Option threads = new Option("t", "threads", true, "");
		this.options.addOption(threads);
		
		Option quiet = new Option("q", "quiet", true, "");
		this.options.addOption(quiet);
		
		Option help = new Option("h", "help", true, "");
		this.options.addOption(help);

		Option version = new Option("v", "version", true, "");
		this.options.addOption(version);
		
		Option cap_mismatch_quals = new Option("", "cap-mismatch-quals", true, "");
		this.options.addOption(cap_mismatch_quals);
		
		Option interleaved_input = new Option("", "interleaved-input", true, "");
		this.options.addOption(interleaved_input);
		
		Option interleaved_output = new Option("", "interleaved-output", true, "");
		this.options.addOption(interleaved_output);
		
		Option compress_prog = new Option("", "compress-prog", true, "");
		this.options.addOption(compress_prog);
		
		Option compress_prog_args = new Option("", "compress-prog-args", true, "");
		this.options.addOption(compress_prog_args);
		
		Option suffix = new Option("", "suffix", true, "");
		this.options.addOption(suffix);

		Option output_suffix = new Option("", "output-suffix", true, "");
		this.options.addOption(output_suffix);
		
		Option numReduceTasks = new Option("n", "numReduceTasks", true, "Number of Reduce");
		this.options.addOption(numReduceTasks);
		
		Option masterHostname = new Option("mH", "masterHostname", true, "The host name of the master node in the hadoop cluster");
		this.options.addOption(masterHostname);
		
		Option clusterPort = new Option("cP", "clusterPort", true, "Rpc port of hdfs in hadoop cluster");
		this.options.addOption(clusterPort);
		
	}
	
	public void parseParameters(String[] args) {

		CommandLineParser parser = new BasicParser();
		CommandLine cmd;

		try {
			cmd = parser.parse(this.options, args);

			if (cmd.hasOption("hi") || cmd.hasOption("hinput")) {
				hinputPath = cmd.getOptionValue("hinput");
			}

			if (cmd.hasOption("ho") || cmd.hasOption("houtput")) {
				houtputPath = cmd.getOptionValue("houtput");
			}
			
			if (cmd.hasOption('m') || cmd.hasOption("min-overlap")) {
				min_overlap = cmd.getOptionValue("min-overlap");
			}
			
			if (cmd.hasOption('M') || cmd.hasOption("max-overlap")) {
				max_overlap = cmd.getOptionValue("max-overlap");
			}
			
			if (cmd.hasOption('x') || cmd.hasOption("max-mismatch-density")) {
				max_mismatch_density = cmd.getOptionValue("max-mismatch-density");
			}
			
			if (cmd.hasOption('O') || cmd.hasOption("allow-outies")) {
				allow_outies = cmd.getOptionValue("allow-outies");
			}
			
			if (cmd.hasOption('p') || cmd.hasOption("phred-offset")) {
				phred_offset = cmd.getOptionValue("phred-offset");
			}
			
			if (cmd.hasOption('r') || cmd.hasOption("read-len")) {
				read_len = cmd.getOptionValue("read-len");
			}
			
			if (cmd.hasOption('f') || cmd.hasOption("fragment-len")) {
				fragment_len = cmd.getOptionValue("fragment-len");
			}
			
			if (cmd.hasOption('s') || cmd.hasOption("fragment-len-stddev")) {
				fragment_len_stddev = cmd.getOptionValue("fragment-len-stddev");
			}
			if (cmd.hasOption('I') || cmd.hasOption("interleaved")) {
				interleaved = cmd.getOptionValue("interleaved");
			}
			
			if (cmd.hasOption("Ti") || cmd.hasOption("tab-delimited-input")) {
				tab_delimited_input = cmd.getOptionValue("tab-delimited-input");
			}
			
			if (cmd.hasOption("To") || cmd.hasOption("tab-delimited-output")) {
				tab_delimited_output = cmd.getOptionValue("tab-delimited-output");
			}
			
			if (cmd.hasOption('o') || cmd.hasOption("output-prefix")) {
				output_prefix = cmd.getOptionValue("output-prefix");
			}
			
			if (cmd.hasOption('d') || cmd.hasOption("output-directory")) {
				output_directory = cmd.getOptionValue("output-directory");
			}
			
			if (cmd.hasOption('c') || cmd.hasOption("to-stdout")) {
				to_stdout = cmd.getOptionValue("to-stdout");
			}
			
			if (cmd.hasOption('z') || cmd.hasOption("compress")) {
				compress = cmd.getOptionValue("compress");
			}
			
			if (cmd.hasOption('t') || cmd.hasOption("threads")) {
				threads = cmd.getOptionValue("threads");
			}
			
			if (cmd.hasOption('q') || cmd.hasOption("quiet")) {
				quiet = cmd.getOptionValue("quiet");
			}
			
			if (cmd.hasOption('h') || cmd.hasOption("help")) {
				help = cmd.getOptionValue("help");
			}
			
			if (cmd.hasOption('v') || cmd.hasOption("version")) {
				version = cmd.getOptionValue("version");
			}
			
			if (cmd.hasOption("cap-mismatch-quals")) {
				cap_mismatch_quals = cmd.getOptionValue("cap-mismatch-quals");
			}
			
			if (cmd.hasOption("interleaved-input")) {
				interleaved_input = cmd.getOptionValue("interleaved-input");
			}
			
			if (cmd.hasOption("interleaved-output")) {
				interleaved_output = cmd.getOptionValue("interleaved-output");
			}
			
			if (cmd.hasOption("compress-prog")) {
				compress_prog = cmd.getOptionValue("compress-prog");
			}
			
			if (cmd.hasOption("compress-prog-args")) {
				compress_prog_args = cmd.getOptionValue("compress-prog-args");
			}
			
			if (cmd.hasOption("suffix")) {
				suffix = cmd.getOptionValue("suffix");
			}
			
			if (cmd.hasOption("output-suffix")) {
				output_suffix = cmd.getOptionValue("output-suffix");
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
			
		} catch (ParseException e) {
			e.printStackTrace();
			System.exit(1);

		}
	}

	public String getHinputPath() {
		return hinputPath;
	}

	public void setHinputPath(String hinputPath) {
		this.hinputPath = hinputPath;
	}

	public String getHoutputPath() {
		return houtputPath;
	}

	public void setHoutputPath(String houtputPath) {
		this.houtputPath = houtputPath;
	}

	public String getMin_overlap() {
		return min_overlap;
	}

	public void setMin_overlap(String min_overlap) {
		this.min_overlap = min_overlap;
	}

	public String getMax_overlap() {
		return max_overlap;
	}

	public void setMax_overlap(String max_overlap) {
		this.max_overlap = max_overlap;
	}

	public String getMax_mismatch_density() {
		return max_mismatch_density;
	}

	public void setMax_mismatch_density(String max_mismatch_density) {
		this.max_mismatch_density = max_mismatch_density;
	}

	public String getAllow_outies() {
		return allow_outies;
	}

	public void setAllow_outies(String allow_outies) {
		this.allow_outies = allow_outies;
	}

	public String getPhred_offset() {
		return phred_offset;
	}

	public void setPhred_offset(String phred_offset) {
		this.phred_offset = phred_offset;
	}

	public String getRead_len() {
		return read_len;
	}

	public void setRead_len(String read_len) {
		this.read_len = read_len;
	}

	public String getFragment_len() {
		return fragment_len;
	}

	public void setFragment_len(String fragment_len) {
		this.fragment_len = fragment_len;
	}

	public String getFragment_len_stddev() {
		return fragment_len_stddev;
	}

	public void setFragment_len_stddev(String fragment_len_stddev) {
		this.fragment_len_stddev = fragment_len_stddev;
	}

	public String getInterleaved() {
		return interleaved;
	}

	public void setInterleaved(String interleaved) {
		this.interleaved = interleaved;
	}

	public String getTab_delimited_input() {
		return tab_delimited_input;
	}

	public void setTab_delimited_input(String tab_delimited_input) {
		this.tab_delimited_input = tab_delimited_input;
	}

	public String getTab_delimited_output() {
		return tab_delimited_output;
	}

	public void setTab_delimited_output(String tab_delimited_output) {
		this.tab_delimited_output = tab_delimited_output;
	}

	public String getOutput_prefix() {
		return output_prefix;
	}

	public void setOutput_prefix(String output_prefix) {
		this.output_prefix = output_prefix;
	}

	public String getOutput_directory() {
		return output_directory;
	}

	public void setOutput_directory(String output_directory) {
		this.output_directory = output_directory;
	}

	public String getTo_stdout() {
		return to_stdout;
	}

	public void setTo_stdout(String to_stdout) {
		this.to_stdout = to_stdout;
	}

	public String getCompress() {
		return compress;
	}

	public void setCompress(String compress) {
		this.compress = compress;
	}

	public String getThreads() {
		return threads;
	}

	public void setThreads(String threads) {
		this.threads = threads;
	}

	public String getQuiet() {
		return quiet;
	}

	public void setQuiet(String quiet) {
		this.quiet = quiet;
	}

	public String getHelp() {
		return help;
	}

	public void setHelp(String help) {
		this.help = help;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getCap_mismatch_quals() {
		return cap_mismatch_quals;
	}

	public void setCap_mismatch_quals(String cap_mismatch_quals) {
		this.cap_mismatch_quals = cap_mismatch_quals;
	}

	public String getInterleaved_input() {
		return interleaved_input;
	}

	public void setInterleaved_input(String interleaved_input) {
		this.interleaved_input = interleaved_input;
	}

	public String getInterleaved_output() {
		return interleaved_output;
	}

	public void setInterleaved_output(String interleaved_output) {
		this.interleaved_output = interleaved_output;
	}

	public String getCompress_prog() {
		return compress_prog;
	}

	public void setCompress_prog(String compress_prog) {
		this.compress_prog = compress_prog;
	}

	public String getCompress_prog_args() {
		return compress_prog_args;
	}

	public void setCompress_prog_args(String compress_prog_args) {
		this.compress_prog_args = compress_prog_args;
	}

	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	public String getOutput_suffix() {
		return output_suffix;
	}

	public void setOutput_suffix(String output_suffix) {
		this.output_suffix = output_suffix;
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

	public Options getOptions() {
		return options;
	}

	public void setOptions(Options options) {
		this.options = options;
	}

	public String[] getCommandLine() {
		return commandLine;
	}

	public void setCommandLine(String[] commandLine) {
		this.commandLine = commandLine;
	}

	@Override
	public String toString() {
		StringBuffer buff = new StringBuffer("");
		for (int i = 0; i < commandLine.length;) {
			String arg = commandLine[i].trim();
			if (arg.equals("-hi") || arg.equals("-ho")) {
				i = i + 2;
			} else {
				i++;
				buff.append(arg + " ");
			}
		}
		return buff.toString();
	}
	
}
