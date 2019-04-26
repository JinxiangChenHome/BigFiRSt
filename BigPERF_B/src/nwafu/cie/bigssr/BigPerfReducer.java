package nwafu.cie.bigssr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import nwafu.cie.common.ClientInfo;

/**
 * author: Jinxiang Chen
 * mail: 631493049@163.com
 * 
 */
public class BigPerfReducer extends Reducer<Text,NullWritable,Text,NullWritable> implements ClientInfo{
	
	
	@Override
	protected void setup(Context context){
		
	}

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
	
	@Override
	protected void cleanup(Context context){
	}
}
