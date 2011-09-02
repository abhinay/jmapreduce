package org.fingertap.jmapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerWrapper extends Reducer<Text,Object,Text,Object> {

	private JReducer jreducer;
	
	public ReducerWrapper() {
	  jreducer = new JReducer();
	}

	public void setup(Context context) throws IOException {
    jreducer.setup(context);
  }
	
	public void reduce(Text key, Iterable<Object> values, Context context) throws IOException {
		jreducer.reduce(key, values, context);
	}
}