import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperWrapper extends Mapper<Object, Text, Text, Text> {
	
	private JMapper jmapper;
	
	public MapperWrapper() {
	  jmapper = new JMapper();
	}

	public void setup(Context context) throws IOException {
	  jmapper.setup(context);
  }
	
	public void map(Object key, Text value, Context context) throws IOException {
	  jmapper.map(key, value, context);
	}
}