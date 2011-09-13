import org.fingertap.jmapreduce.JMapReduce

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil

JMapReduce.job "HBase bulk import job" do
  reduce_tasks 0
  
  custom_job do |conf|
    hbase_conf = HBaseConfiguration.create(conf)
    hbase_conf.set('hbase.zookeeper.quorum', 'hbase.server.address')
    job = Job.new(hbase_conf, "HBase bulk import job")
    
    TableMapReduceUtil.initTableReducerJob(property('table_name'), nil, job)
    TableMapReduceUtil.addDependencyJars(job)
    TableMapReduceUtil.addDependencyJars(job.getConfiguration)
    
    job.setMapOutputValueClass(Put.java_class)
    job
  end
  
  setup do
    @family = "someColumnFamily".to_java_bytes
    @ts = java.lang.System.currentTimeMillis
  end
  
  map do |key, value|
    row = "#{key}\t#{value}".split("\t")
    
    row_key = row[0].to_java_bytes
    someColumn = row[1].to_java_bytes
    someOtherColumn = row[2].to_java_bytes
    
    put = Put.new(row_key)
    put.add(@family, "columnName".to_java_bytes, @ts, someColumn)
    put.add(@family, "anotherColumnName".to_java_bytes, @ts, someOtherColumn)
    
    context.write(ImmutableBytesWritable.new(row_key), put)
  end
end

__END__

./bin/jmapreduce examples/hbase_import.rb /path/to/tsv/file /output/path -l $HBASE_HOME/hbase.jar,$HBASE_HOME/lib/zookeeper.jar,$HBASE_HOME/lib/guava.jar -v table_name=test_import_table