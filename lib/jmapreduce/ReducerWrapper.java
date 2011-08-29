import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerWrapper extends Reducer<Text,Text,Text,Text> {

	private JReducer jreducer;
	
	public ReducerWrapper() {
	  jreducer = new JReducer();
	}

	public void setup(Context context) throws IOException {
    jreducer.setup(context);
  }
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
		jreducer.reduce(key, values, context);
	}
}