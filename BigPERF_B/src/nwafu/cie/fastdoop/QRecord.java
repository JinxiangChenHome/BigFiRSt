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

/**
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0
 * 
 *          Date: Nov, 22 2016
 * 
 *          Utility class used to represent as a record a sequence existing in a
 *          FASTQ file
 * 
 */
public class QRecord {

	public byte[] buffer;
	public int startKey, endKey, startValue, endValue, startKey2, endKey2, startQuality, endQuality;
	public boolean keyReply;

	public QRecord() {
		super();
	}

	public String getKey() {
		return new String(buffer, startKey, (endKey - startKey + 1));
	}

	public String getValue() {
		return new String(buffer, startValue, (endValue - startValue + 1));
	}

	public String getKey2() {
		return new String(buffer, startKey2, (endKey2 - startKey2 + 1));
	}

	public String getQuality() {
		return new String(buffer, startQuality, (endQuality - startQuality + 1));
	}

	@Override
	public String toString() {

		return "@" + this.getKey() + "\n" + this.getValue() + "\n+" + this.getKey2() + "\n" + this.getQuality() + "\n";

	}

	public byte[] getBuffer() {
		return buffer;
	}

	public int getStartValue() {
		return startValue;
	}

	public int getEndValue() {
		return endValue;
	}

}
