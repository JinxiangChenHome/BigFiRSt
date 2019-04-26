#!/usr/bin/env python
#coding=utf-8
import time
import datetime

import argparse

# author: Jinxiang Chen mail: 631493049@163.com

parser = argparse.ArgumentParser()
parser._action_groups.pop()
required = parser.add_argument_group('Required arguments')
required.add_argument('-i1', '--input1', required=True, metavar='<FILE>', help='Input file in FASTA format')
required.add_argument('-i2', '--input2', required=True, metavar='<FILE>', help='Input file in FASTA format')
required.add_argument('-o', '--output', required=True, metavar='<FILE>', help='Input file in FASTA format')
args = parser.parse_args()




fp1 = open(args.input1,'r')
fp2 = open(args.input2,'r')
fw = open(args.output,'w+')

nowTime = datetime.datetime.now()
i = 1
while 1:
	lines = fp1.readlines(3000000)
	if not lines:
		break
	for line in lines:
		if(i % 4 != 2 and i % 4 != 0):
			file1 = line.decode().strip()
			file2 = fp2.readline().decode().strip()
			fw.write(file1 + "\n")
		else:
			file1 = line.decode().strip()
			file2 = fp2.readline().decode().strip()
			fw.write(file1 + ",SEQ," + file2 + "\n")
		i += 1

fw.close()
fp2.close()
fp1.close()
nowTime2 = datetime.datetime.now()
