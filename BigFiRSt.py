#coding=utf-8

import argparse

import subprocess
import os

# author: Jinxiang Chen mail: 631493049@163.com

def getArgs():
	
	__version__ = 'V 1.0'
	parser = argparse.ArgumentParser()
	parser._action_groups.pop()

	required = parser.add_argument_group('Required arguments')
	
	required.add_argument('-hh', '--hadoop_home', required=True, metavar='<PATH>', help='Give the Hadoop installation path (*** Must be specified ***)')
	required.add_argument('-mH', '--masterHostname', required=True, metavar='<PATH>', help='The host name of the master node in the hadoop cluster (*** Must be specified ***)')
	required.add_argument('-cP', '--clusterPort', required=True, metavar='<INT>', help='Rpc port of hdfs in hadoop cluster (*** Must be specified ***)')
	required.add_argument('-fj', '--fjar_home', metavar='<PATH>', help='Give the jar file corresponding to the algorithm running in the Hadoop cluster (*** This parameter must be specified when running BigFLASH or BigFiRSt ***)')
	required.add_argument('-fmc', '--fmain_class', metavar='<PATH>', help='Give the main class of the running algorithm (*** This parameter must be specified when running BigFLASH or BigFiRSt ***)')
	required.add_argument('-pj', '--pjar_home', metavar='<PATH>', help='Give the jar file corresponding to the BigPERF algorithm running in the Hadoop cluster (*** This parameter must be specified when running BigPERF or BigFiRSt ***)')
	required.add_argument('-pmc', '--pmain_class', metavar='<PATH>', help='Give the main class of the running BigPERF algorithm (*** This parameter must be specified when running BigPERF or BigFiRSt ***)')

	method_group = required.add_mutually_exclusive_group()
	method_group.add_argument('-bf', '--big_flash', action="store_true", help='Only use BigFlash (*** This parameter must be specified when running BigFLASH ***)')
	method_group.add_argument('-bp', '--big_perf', action="store_true", help='Only use BigPERF (*** This parameter must be specified when running BigPERF***)')
	method_group.add_argument('-bfp', '--big_flash_perf', action="store_true", help='Mixing BigFlash with BigPERF (*** This parameter must be specified when running BigFiRSt ***)')

	flashOptional = parser.add_argument_group('BigFLASH optional arguments')
	flashOptional.add_argument('-fi1', '--finput1', metavar='<FILE>', help='Input file in FASTQ format (*** This parameter must be specified when running BigFLASH or BigFiRSt ***)')
	flashOptional.add_argument('-fi2', '--finput2', metavar='<FILE>', help='Input file in FASTQ format (*** This parameter must be specified when running BigFLASH or BigFiRSt ***)')
	flashOptional.add_argument('-fhi', '--fhadoop_input', metavar='<PATH>', help='Give the input path of BigFlash (*** This parameter must be specified when running BigFLASH or BigFiRSt ***)')
	flashOptional.add_argument('-fho', '--fhadoop_output', metavar='<PATH>', help='Give the output path of BigFlash (*** This parameter must be specified when running BigFLASH or BigFiRSt ***)')
	
	flashOptional.add_argument('-fm', '--fmin_overlap', metavar='<NUM>', help='Specifies the minimum overlap length between two reads that will be merged. Default: 10bp')
	
	flashOptional.add_argument('-fM', '--fmax_overlap', metavar='<NUM>', help='Specifies the maximum overlap length between two reads that will be merged. Default: 65bp')
	
	flashOptional.add_argument('-fx', '--fmax_mismatch_density', metavar='<NUM>', help='Specifies the maximum allowable ratio between the number of base pairs that do not match and the length of the overlap. Default: 0.25')
	
	flashOptional.add_argument('-fO', '--fallow_outies', help='Specify the direction of the combining read pairs')
	flashOptional.add_argument('-fp', '--fphred_offset', metavar='<OFFSET>', help='Specifies the minimum ASCII value that is typically used to represent the character of the base quality value in the FASTQ file. Default: 33')
	
	flashOptional.add_argument('-fr', '--fread_len', metavar='<LEN>', help='Read length')
	flashOptional.add_argument('-ff', '--ffragment_len', metavar='<LEN>', help='Fragment length')
	flashOptional.add_argument('-fs', '--ffragment_len_stddev', metavar='<LEN>', help='This parameter is used to specify average read length, fragment length, and fragment standard deviation. Default: -r 100, -f 180, -s 18')
	flashOptional.add_argument('-fI', '--finterleaved', help='This parameter is used, which is equivalent to using --finterleaved-input and --finterleaved-output')
	flashOptional.add_argument('-fTi', '--ftab_delimited_input', help='This parameter is used to specify an input file in tab-delimited format')
	flashOptional.add_argument('-fTo', '--ftab_delimited_output', help='This parameter is used to specify an output file in tab-delimited format')
	flashOptional.add_argument('-fo', '--foutput_prefix', metavar='<PREFIX>', help='This parameter is used to specify the prefix of the output file. Default: "out"')
	flashOptional.add_argument('-fd', '--foutput_directory', metavar='<DIR>', help='This parameter is used to specify the path of the output file. Default: program running directory')
	flashOptional.add_argument('-fc', '--fto_stdout', help='This parameter specifies the combined read write standard output')
	flashOptional.add_argument('-fz', '--fcompress', help='This parameter is used to specify that the output file is compressed directly using zlib')
	flashOptional.add_argument('-ft', '--fthreads', metavar='<NTHREADS>', help="This parameter is used to set the number of threads. Default: number of cores")
	
	flashOptional.add_argument('-fcap', '--fcap_mismatch_quals', help='This parameter is used to set the quality score of the mismatched position assignment')
	
	flashOptional.add_argument('-finterleavedi', '--finterleaved_input', help='The files MATES_1.FASTQ and MATES_2.FASTQ are not required, but a single file MATES.FASTQ is allowed to interleave the paired reads. Specify " - " to parse the input parameters')
	flashOptional.add_argument('-finterleavedo', '--finterleaved_output', help='The write interleaved FASTQ format is not combined')
	flashOptional.add_argument('-fcompress', '--fcompress_prog', metavar='<PROG>', help="The tube is output via the compression program PROG, which named `PROG-c - `,and the pipe will also transfer any parameters specified by --compress-prog-args")
	flashOptional.add_argument('-fcompressa', '--fcompress_prog_args', metavar='<ARGS>', help="If a string of extra arguments is clarified with --compress-prog=PROG,it will be passed to the compression program")
	flashOptional.add_argument('-fsuffix', '--fsuffix', metavar='<SUFFIX>', help="The suffix after the '.fastq' of the output file uses SUFFIX. Default: none")
	flashOptional.add_argument('-foutput', '--foutput_suffix', metavar='<SUFFIX>', help="The suffix after the '.fastq' of the output file uses SUFFIX. Default: none")
	flashOptional.add_argument('-fn', '--fnumReduceTasks', metavar='<INT>', help='Number of Reduce')
	

	perfOptional = parser.add_argument_group('BigPERF optional arguments')
	perfOptional.add_argument('-pi', '--pinput', metavar='<FILE>', help='Input file in FASTA format (*** This parameter must be specified when running BigPERF ***)')
	perfOptional.add_argument('-phi', '--phadoop_input', metavar='<PATH>', help='Give the input path of BigPERF (*** This parameter must be specified when running BigPERF or BigFiRSt ***)')
	perfOptional.add_argument('-pho', '--phadoop_output', metavar='<PATH>', help='Give the output path of BigPERF (*** This parameter must be specified when running BigPERF or BigFiRSt ***)')
	perfOptional.add_argument('-pm', '--pmin_motif_size', metavar='<INT>', help='The minimum size of the motif to be mined in the sequence. Default: 3')
	perfOptional.add_argument('-pM', '--pmax_motif_size', metavar='<INT>', help='The Maximum size of the motif to be mined in the sequence. Default: 5')
	perfOptional.add_argument('-pl', '--pmin_length', metavar='<INT>', help='Specify a minimum length cutoff. Default: 6')
	perfOptional.add_argument('-ps', '--pmin_seq_length', metavar='<INT>', help='Minimum sequence length to be mined. Default: 0')
	perfOptional.add_argument('-pS', '--pmax_seq_length', metavar='<INT>', help='Maximum sequence length to be mined. Default: 2147483646')
	perfOptional.add_argument('-pu', '--pmin_units', metavar='<PATH>', help='Specify the minimum number of repeating units to be mined. This parameter can be an integer or a file')
	perfOptional.add_argument('-pr', '--prepeats', metavar='<PATH>', help='Specify a file with pre-specified repeats (Cannot be used with the -pm and/or -pM)')
	perfOptional.add_argument('-pf', '--pfilter_seq_ids', metavar='<PATH>', help='Filter some sequences by the ID of the given file of the user')
	perfOptional.add_argument('-pF', '--ptarget_seq_ids', metavar='<PATH>', help='Process some sequences by the ID of the given file of the user')
	perfOptional.add_argument('-pn', '--pnumReduceTasks', metavar='<INT>', help='Number of Reduce')
	perfOptional.add_argument('-pa', '--panalyse', metavar='<PATH>', help='Generate a HTML analysis file')
	perfOptional.add_argument('-pfl', '--pflank_len', metavar='<INT>', help='Length of terminal flanking sequences')

	perfOptional.add_argument('--version', action='version', version='BigFlash_PERF ' + __version__)

	args = parser.parse_args()

	return args

def main():
	args = getArgs()

	pre_py = "pre_sequence.py"
	pre_outfile_flash = "aft_res.txt"
	
	hadoop_home = args.hadoop_home
	

	if args.big_flash:
		flash_command = ' -hi ' + args.fhadoop_input + '/' + pre_outfile_flash + ' -ho ' + args.fhadoop_output + ' -mH ' + args.masterHostname + ' -cP ' + args.clusterPort
		
		
		if args.fmin_overlap:
			flash_command = flash_command + ' -m ' + args.fmin_overlap
		if args.fmax_overlap:
			flash_command = flash_command + ' -M ' + args.fmax_overlap
		if args.fmax_mismatch_density:
			flash_command = flash_command + ' -x ' + args.fmax_mismatch_density
		if args.fallow_outies:
			flash_command = flash_command + ' -O '
		if args.fphred_offset:
			flash_command = flash_command + ' -p ' + args.fphred_offset
		if args.fread_len:
			flash_command = flash_command + ' -r ' + args.fread_len
		if args.ffragment_len:
			flash_command = flash_command + ' -f ' + args.ffragment_len
		if args.ffragment_len_stddev:
			flash_command = flash_command + ' -s ' + args.ffragment_len_stddev
		if args.finterleaved:
			flash_command = flash_command + ' -I '
		if args.ftab_delimited_input:
			flash_command = flash_command + ' -Ti '
		if args.ftab_delimited_output:
			flash_command = flash_command + ' -To '
		if args.foutput_prefix:
			flash_command = flash_command + ' -o ' + args.foutput_prefix
		if args.foutput_directory:
			flash_command = flash_command + ' -d ' + args.foutput_directory
		if args.fto_stdout:
			flash_command = flash_command + ' -c '
		if args.fcompress:
			flash_command = flash_command + ' -z '
		if args.fthreads:
			flash_command = flash_command + ' -t ' + args.fthreads
		if args.fcap_mismatch_quals:
			flash_command = flash_command + ' --cap-mismatch-quals '
		if args.finterleaved_input:
			flash_command = flash_command + ' --interleaved-input '
		if args.finterleaved_output:
			flash_command = flash_command + ' --interleaved-output '
		if args.fcompress_prog:
			flash_command = flash_command + ' --compress-prog ' + args.fcompress_prog
		if args.fcompress_prog_args:
			flash_command = flash_command + ' --compress-prog-args ' + args.fcompress_prog_args
		if args.fsuffix:
			flash_command = flash_command + ' --suffix ' + args.fsuffix
		if args.foutput_suffix:
			flash_command = flash_command + ' --output-suffix ' + args.foutput_suffix
		if args.fnumReduceTasks:
			flash_command = flash_command + ' -n ' + args.fnumReduceTasks

		
		# Python call shell command
		subprocess.call('python ' + pre_py + ' -i1 ' + args.finput1 + ' -i2 ' + args.finput2 + ' -o ' + pre_outfile_flash ,shell=True)
		
		
		# Upload pre-processed files to HDFS
		subprocess.call(hadoop_home + '/bin/hadoop fs -put ' + pre_outfile_flash + ' ' + args.fhadoop_input,shell=True)
		
		if os.path.exists(pre_outfile_flash):
		    os.remove(pre_outfile_flash)
		else:
		    print 'no such file:%s'%pre_outfile_flash

		# Submit a task to the cluster
		subprocess.call(hadoop_home + '/bin/yarn jar ' + args.fjar_home+ ' ' + args.fmain_class + flash_command,shell=True)

		

	if args.big_perf:
		perf_command = ' -i ' + args.phadoop_input + '/*' + ' -o ' + args.phadoop_output + ' -mH ' + args.masterHostname + ' -cP ' + args.clusterPort
		if args.pmin_motif_size:
			perf_command = perf_command + ' -m ' + args.pmin_motif_size
		if args.pmax_motif_size:
			perf_command = perf_command + ' -M ' + args.pmax_motif_size
		if args.pmin_length:
			perf_command = perf_command + ' -l ' + args.pmin_length
		if args.pmin_seq_length:
			perf_command = perf_command + ' -s ' + args.pmin_seq_length
		if args.pmax_seq_length:
			perf_command = perf_command + ' -S ' + args.pmax_seq_length
		if args.pmin_units:
			perf_command = perf_command + ' -u ' + args.pmin_units
		if args.prepeats:
			perf_command = perf_command + ' -r ' + args.prepeats
		if args.pfilter_seq_ids:
			perf_command = perf_command + ' -f ' + args.pfilter_seq_ids
		if args.ptarget_seq_ids:
			perf_command = perf_command + ' -F ' + args.ptarget_seq_ids
		if args.pnumReduceTasks:
			perf_command = perf_command + ' -n ' + args.pnumReduceTasks
		if args.panalyse:
			perf_command = perf_command + ' -a ' + args.panalyse
		if args.pflank_len:
			perf_command = perf_command + ' -fl ' + args.pflank_len

		
		# Upload data to HDFS
		subprocess.call(hadoop_home + '/bin/hadoop fs -put ' + args.pinput + ' ' + args.phadoop_input,shell=True)
		
		
		# Submit a task to the cluster
		subprocess.call(hadoop_home + '/bin/yarn jar ' + args.pjar_home + ' ' + args.pmain_class + perf_command,shell=True)

		

	if args.big_flash_perf:
		
		flash_command = ' -hi ' + args.fhadoop_input + '/' + pre_outfile_flash + ' -ho ' + args.fhadoop_output + ' -mH ' + args.masterHostname + ' -cP ' + args.clusterPort

		if args.fmin_overlap:
			flash_command = flash_command + ' -m ' + args.fmin_overlap
		if args.fmax_overlap:
			flash_command = flash_command + ' -M ' + args.fmax_overlap
		if args.fmax_mismatch_density:
			flash_command = flash_command + ' -x ' + args.fmax_mismatch_density
		if args.fallow_outies:
			flash_command = flash_command + ' -O '
		if args.fphred_offset:
			flash_command = flash_command + ' -p ' + args.fphred_offset
		if args.fread_len:
			flash_command = flash_command + ' -r ' + args.fread_len
		if args.ffragment_len:
			flash_command = flash_command + ' -f ' + args.ffragment_len
		if args.ffragment_len_stddev:
			flash_command = flash_command + ' -s ' + args.ffragment_len_stddev
		if args.finterleaved:
			flash_command = flash_command + ' -I '
		if args.ftab_delimited_input:
			flash_command = flash_command + ' -Ti '
		if args.ftab_delimited_output:
			flash_command = flash_command + ' -To '
		if args.foutput_prefix:
			flash_command = flash_command + ' -o ' + args.foutput_prefix
		if args.foutput_directory:
			flash_command = flash_command + ' -d ' + args.foutput_directory
		if args.fto_stdout:
			flash_command = flash_command + ' -c '
		if args.fcompress:
			flash_command = flash_command + ' -z '
		if args.fthreads:
			flash_command = flash_command + ' -t ' + args.fthreads
		if args.fcap_mismatch_quals:
			flash_command = flash_command + ' --cap-mismatch-quals '
		if args.finterleaved_input:
			flash_command = flash_command + ' --interleaved-input '
		if args.finterleaved_output:
			flash_command = flash_command + ' --interleaved-output '
		if args.fcompress_prog:
			flash_command = flash_command + ' --compress-prog ' + args.fcompress_prog
		if args.fcompress_prog_args:
			flash_command = flash_command + ' --compress-prog-args ' + args.fcompress_prog_args
		if args.fsuffix:
			flash_command = flash_command + ' --suffix ' + args.fsuffix
		if args.foutput_suffix:
			flash_command = flash_command + ' --output-suffix ' + args.foutput_suffix
		if args.fnumReduceTasks:
			flash_command = flash_command + ' -n ' + args.fnumReduceTasks
		
		perf_command = ' -i ' + args.fhadoop_output + '/file/*' + ' -o ' + args.phadoop_output + ' -mH ' + args.masterHostname + ' -cP ' + args.clusterPort
		if args.pmin_motif_size:
			perf_command = perf_command + ' -m ' + args.pmin_motif_size
		if args.pmax_motif_size:
			perf_command = perf_command + ' -M ' + args.pmax_motif_size
		if args.pmin_length:
			perf_command = perf_command + ' -l ' + args.pmin_length
		if args.pmin_seq_length:
			perf_command = perf_command + ' -s ' + args.pmin_seq_length
		if args.pmax_seq_length:
			perf_command = perf_command + ' -S ' + args.pmax_seq_length
		if args.pmin_units:
			perf_command = perf_command + ' -u ' + args.pmin_units
		if args.prepeats:
			perf_command = perf_command + ' -r ' + args.prepeats
		if args.pfilter_seq_ids:
			perf_command = perf_command + ' -f ' + args.pfilter_seq_ids
		if args.ptarget_seq_ids:
			perf_command = perf_command + ' -F ' + args.ptarget_seq_ids
		if args.pnumReduceTasks:
			perf_command = perf_command + ' -n ' + args.pnumReduceTasks
		if args.panalyse:
			perf_command = perf_command + ' -a ' + args.panalyse
		if args.pflank_len:
			perf_command = perf_command + ' -fl ' + args.pflank_len

		
		# Python call shell command
		subprocess.call('python ' + pre_py + ' -i1 ' + args.finput1 + ' -i2 ' + args.finput2 + ' -o ' + pre_outfile_flash ,shell=True)
		
		
		# Upload pre-processed files to HDFS
		subprocess.call(hadoop_home + '/bin/hadoop fs -put ' + pre_outfile_flash + ' ' + args.fhadoop_input,shell=True)
		
		if os.path.exists(pre_outfile_flash):
		    os.remove(pre_outfile_flash)
		else:
		    print 'no such file:%s'%pre_outfile_flash
		
		# Submit a task to the cluster
		subprocess.call(hadoop_home + '/bin/yarn jar ' + args.fjar_home + ' ' + args.fmain_class + flash_command,shell=True)

		
		# Submit a task to the cluster
		subprocess.call(hadoop_home + '/bin/yarn jar ' + args.pjar_home + ' ' + args.pmain_class + perf_command,shell=True)


if __name__ == '__main__':
	main()


