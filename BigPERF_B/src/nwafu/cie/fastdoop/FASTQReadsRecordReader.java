/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nwafu.cie.fastdoop;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0
 * 
 *          Date: Nov, 22 2016
 * 
 *          This class reads <key, value> pairs from an InputSplit.
 *          The input file is in FASTQ format.
 *          A FASTA record has a header line that is the key, the data line, an
 *          optional single
 *          line header string and a quality line
 * 
 *          Example:
 * 			@SRR034939.184 090406_HWI-EAS68_9096_FC400PR_PE_1_1_10 length=100
 *          CCACCTCCTGGGTTCAAGGGGTTCTCTTGCCTCAGCTNNNNNNNNNNNNGGNNNNNNNNNTNNNN
 *          +SRR034939.184 090406_HWI-EAS68_9096_FC400PR_PE_1_1_10 length=100
 *          HDHFHHHHHHFFAFF6?<:<HHHHHHHHHEDHHHF##!!!!!!!!!!!!##!!!!!!!!!#!!!!
 * 
 *          ...
 */

public class FASTQReadsRecordReader extends RecordReader<NullWritable, QRecord> {

	private FSDataInputStream inputFile;

	private long startByte;

	private NullWritable currKey;

	private QRecord currRecord;

	public static final int KV_BUFFER_SIZE = 4096;

	/*
	 * Used to buffer the content of the input split
	 */
	private byte[] myInputSplitBuffer;

	/*
	 * Auxiliary buffer used to store the ending buffer of this input split and
	 * the initial bytes of the next split
	 */
	private byte[] borderBuffer;

	/*
	 * Marks the current position in the input split buffer
	 */
	private int posBuffer;

	/*
	 * Stores the size of the input split buffer
	 */
	private int sizeBuffer;
	/*
	 * True, if we processed the entire input split buffer. False, otherwise
	 */
	private boolean endMyInputSplit = false;

	boolean isLastSplit = false;

	public FASTQReadsRecordReader() {
		super();
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {// Called once at
														// initialization.

		posBuffer = 0;
		Configuration job = context.getConfiguration();

		/*
		 * We open the file corresponding to the input split and
		 * start processing it
		 */
		FileSplit split = (FileSplit) genericSplit;
		Path path = split.getPath();
		startByte = split.getStart();
		inputFile = path.getFileSystem(job).open(path);
		inputFile.seek(startByte);

		currKey = NullWritable.get();
		currRecord = new QRecord();

		/*
		 * We read the whole content of the split in memory using
		 * myInputSplitBuffer. Plus, we read in the memory the first
		 * KV_BUFFER_SIZE of the next split
		 */
		myInputSplitBuffer = new byte[(int) split.getLength()];
		currRecord.buffer = myInputSplitBuffer;

		borderBuffer = new byte[KV_BUFFER_SIZE];

		sizeBuffer = inputFile.read(startByte, myInputSplitBuffer, 0, myInputSplitBuffer.length);
		inputFile.seek(startByte + sizeBuffer);

		if (inputFile.available() == 0) {
			isLastSplit = true;
			int newLineCount = 0;
			int k = 1;

			while (true) {
				if (myInputSplitBuffer[myInputSplitBuffer.length - k] == '\n') {
					k++;
					newLineCount++;
				} else
					break;

			}

			byte[] tempBuffer = new byte[(int) split.getLength() - newLineCount];
			System.arraycopy(myInputSplitBuffer, 0, tempBuffer, 0, myInputSplitBuffer.length - newLineCount);
			myInputSplitBuffer = tempBuffer;
		}

		for (int i = 0; i < sizeBuffer; i++) {
			if (myInputSplitBuffer[i] == '@') {
				if (i == 0) {
					posBuffer = i + 1;
					break;
				}
				if (myInputSplitBuffer[i - 1] == '\n') {
					posBuffer = i + 1;
					break;
				}

			}
		}

		/*
		 * We skip the first header of the split
		 */
		int j = posBuffer + 1;

		while (myInputSplitBuffer[j] != '\n') {
			j++;
		}

		if (myInputSplitBuffer[j + 1] == '@')
			posBuffer = j + 2;

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (endMyInputSplit) {
			return false;
		}

		boolean nextsplitKey = false;
		boolean nextsplitValue = false;
		boolean nextsplitQuality = false;
		boolean nextsplitSecondHeader = false;

		currRecord.startKey = posBuffer;

		/*
		 * We look for the next short sequence my moving posBuffer until a
		 * newline character is found.
		 * End of split is implicitly managed through
		 * ArrayIndexOutOfBoundsException handling
		 */

		try {
			while (myInputSplitBuffer[posBuffer] != '\n') {
				posBuffer++;
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			/*
			 * If we reached the end of the split while scanning a sequence, we
			 * use nextsplitKey to remember that more characters have to be
			 * fetched from the next split for retrieving the key
			 */
			if (!isLastSplit) {
				endMyInputSplit = true;
				nextsplitKey = true;
			} else {
				return false;
			}
		}

		currRecord.endKey = (posBuffer - 1);

		if (!endMyInputSplit) {
			/*
			 * Assuming there are more characters from the current split to
			 * process, we move forward the pointer
			 * until the symbol '+' is found
			 */

			currRecord.startValue = (posBuffer + 1);

			try {
				posBuffer = posBuffer + 2;

				while (myInputSplitBuffer[posBuffer] != '+') {
					posBuffer++;
				}

				currRecord.endValue = (posBuffer - 2);
				posBuffer++;

			} catch (ArrayIndexOutOfBoundsException e) {

				if (isLastSplit) {
					return false;
				}

				/*
				 * If we reached the end of the split while scanning a sequence,
				 * we use nextsplitValue to remember that more characters have
				 * to be fetched from the next split for retrieving the value
				 */

				endMyInputSplit = true;
				nextsplitValue = true;
				int c = 0;

				if (posBuffer > myInputSplitBuffer.length) {
					posBuffer = myInputSplitBuffer.length;
				}

				for (int i = posBuffer - 1; i >= 0; i--) {
					if (((char) myInputSplitBuffer[i]) != '\n')
						break;

					c++;
				}

				currRecord.endValue = (posBuffer - 1) - c;

			}

		}

		if (!endMyInputSplit) {

			currRecord.startKey2 = posBuffer;

			try {

				try {
					while (myInputSplitBuffer[posBuffer] != '\n') {
						posBuffer++;
					}
				} catch (ArrayIndexOutOfBoundsException e) {

					if (isLastSplit) {
						return false;
					}
					/*
					 * If we reached the end of the split while scanning a
					 * sequence,
					 * we use nextsplitQuality to remember that more characters
					 * have
					 * to be fetched from the next split for retrieving (and
					 * discarding)
					 * the quality linevalue
					 */
					endMyInputSplit = true;
					nextsplitSecondHeader = true;
					nextsplitQuality = true;
				}
				currRecord.endKey2 = posBuffer - 1;

				if (!endMyInputSplit) {

					currRecord.startQuality = (posBuffer + 1);
					currRecord.endQuality = (currRecord.startQuality + (currRecord.endValue - currRecord.startValue));
					posBuffer = (currRecord.endQuality + 3);

					if (myInputSplitBuffer.length <= currRecord.endQuality) {

						currRecord.endQuality = (myInputSplitBuffer.length - 1);
						posBuffer = (myInputSplitBuffer.length - 1);

						throw new ArrayIndexOutOfBoundsException();
					} else {
						if (posBuffer > (myInputSplitBuffer.length - 1)) {
							endMyInputSplit = true;
							return true;
						}
					}
				}

			} catch (ArrayIndexOutOfBoundsException e) {
				if (isLastSplit) {
					return false;
				}

				endMyInputSplit = true;
				nextsplitQuality = true;
			}

		}

		/*
		 * The end of the split has been reached
		 */
		if (endMyInputSplit) {

			/*
			 * If there is another split after this one and we still need to
			 * retrieve the
			 * key of the current record, we switch to borderbuffer to fetch all
			 * the remaining characters
			 */

			if (nextsplitKey) {
				currRecord.buffer = (borderBuffer);
				int j = posBuffer - currRecord.startKey;
				System.arraycopy(myInputSplitBuffer, currRecord.startKey, borderBuffer, 0, j);

				posBuffer = j;

				currRecord.startKey = 0;
				nextsplitValue = true;

				try {

					while (true) {

						byte b = (byte) inputFile.readByte();

						if (b == '\n') {
							break;
						}

						borderBuffer[j++] = b;
					}
				} catch (EOFException e) {
					nextsplitValue = false;
				}

				if (!nextsplitValue) {
					return false;
				}

				currRecord.endKey = (j - 1);

			}

			/*
			 * If there is another split after this one and we still need to
			 * retrieve the value of the current record, we switch to
			 * borderbuffer to fetch all the remaining characters
			 */
			if (nextsplitValue) {

				if (!nextsplitKey) {

					currRecord.buffer = borderBuffer;

					int j = currRecord.endKey + 1 - currRecord.startKey;
					System.arraycopy(myInputSplitBuffer, currRecord.startKey, borderBuffer, 0, j);

					currRecord.startKey = 0;
					currRecord.endKey = (j - 1);

					int start = currRecord.startValue;
					currRecord.startValue = j;

					if ((currRecord.endValue + 1 - start) > 0)
						System.arraycopy(myInputSplitBuffer, start, borderBuffer, j, (currRecord.endValue + 1 - start));

					if ((currRecord.endValue - start) < 0) {
						posBuffer = j;
					} else {
						posBuffer = j + currRecord.endValue - start;
					}

					currRecord.endValue = posBuffer;

					posBuffer++;

				} else {
					posBuffer = currRecord.endKey + 1;
					currRecord.startValue = posBuffer;
				}

				try {
					while (true) {

						byte b = (byte) inputFile.readByte();

						if (b == '+')
							break;

						if (b != '\n')
							borderBuffer[posBuffer++] = b;
					}
				} catch (EOFException e) {

				}

				currRecord.endValue = (posBuffer - 1);

				posBuffer++;
				currRecord.startKey2 = posBuffer;

				try {

					while (true) {
						byte b = (byte) inputFile.readByte();
						if (b == '\n')
							break;
						borderBuffer[posBuffer++] = b;

					}
					currRecord.endKey2 = posBuffer - 1;

					currRecord.startQuality = posBuffer;

					while (true) {
						byte b = (byte) inputFile.readByte();

						if (b != '\n') {
							borderBuffer[posBuffer++] = b;
						} else
							break;
					}

				} catch (EOFException e) {
					// End file.
				}

				currRecord.endQuality = (posBuffer - 1);

			}

			/*
			 * If there is another split after this one and we still need to
			 * retrieve the quality line of the current record, we switch to
			 * borderbuffer to fetch all the remaining characters
			 */
			if (nextsplitQuality) {

				currRecord.buffer = borderBuffer;

				// copy key
				int j = currRecord.endKey + 1 - currRecord.startKey;
				System.arraycopy(myInputSplitBuffer, currRecord.startKey, borderBuffer, 0, j);

				currRecord.startKey = 0;
				currRecord.endKey = (j - 1);

				// copy value
				int v = currRecord.endValue + 1 - currRecord.startValue;
				System.arraycopy(myInputSplitBuffer, currRecord.startValue, borderBuffer, j, v);

				currRecord.startValue = j;
				currRecord.endValue = (j + v - 1);

				if (nextsplitSecondHeader) {
					int start = currRecord.startKey2;
					currRecord.startKey2 = (currRecord.endValue + 1);
					posBuffer = currRecord.startKey2;

					if ((currRecord.endKey2 + 1 - start) > 0)
						System.arraycopy(myInputSplitBuffer, start, borderBuffer, currRecord.startKey2,
								(currRecord.endKey2 + 1 - start));

					posBuffer = currRecord.startKey2 + (currRecord.endKey2 - start);

					currRecord.endKey2 = posBuffer;
					posBuffer++;

					while (true) {

						byte b = (byte) inputFile.readByte();

						if (b == '\n')
							break;
						borderBuffer[posBuffer++] = b;

					}
					currRecord.endKey2 = posBuffer - 1;
					currRecord.startQuality = posBuffer;

				} else {

					int s = currRecord.endKey2 + 1 - currRecord.startKey2;
					System.arraycopy(myInputSplitBuffer, currRecord.startKey2, borderBuffer, currRecord.endValue + 1,
							s);
					currRecord.startKey2 = currRecord.endValue + 1;
					currRecord.endKey2 = (currRecord.startKey2 + s - 1);

					int start = currRecord.startQuality;
					currRecord.startQuality = (currRecord.endKey2 + 1);
					posBuffer = currRecord.startQuality;

					if ((currRecord.endQuality + 1 - start) > 0)
						System.arraycopy(myInputSplitBuffer, start, borderBuffer, currRecord.startQuality,
								(currRecord.endQuality + 1 - start));

					posBuffer = currRecord.startQuality + (currRecord.endQuality - start);

					currRecord.endQuality = posBuffer;
					posBuffer++;
				}

				try {
					while (true) {

						byte b = (byte) inputFile.readByte();

						if (b != '\n')
							borderBuffer[posBuffer++] = b;
						else
							break;
					}
				} catch (EOFException e) {
				}

				currRecord.endQuality = (posBuffer - 1);

			}

		}

		return true;

	}

	@Override
	public void close() throws IOException {

		if (inputFile != null)
			inputFile.close();
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return currKey;
	}

	@Override
	public QRecord getCurrentValue() throws IOException, InterruptedException {
		return currRecord;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return sizeBuffer > 0 ? posBuffer / sizeBuffer : 1;

	}

}