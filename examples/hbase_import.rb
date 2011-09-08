import org.fingertap.jmapreduce.JMapReduce

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.PutSortReducer
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil

JMapReduce.job 'Count' do
  
  customize_job do |job|
    hbase_conf = HBaseConfiguration.create(conf)
    hbase_conf.set('hbase.zookeeper.quorum', 'hbase-master.hadoop.forward.co.uk')
    job.setConfiguration(hbase_conf)
    
    table = HTable.new(hbase_conf, 'ask_pt_keywords_fake')
    job.setReducerClass(PutSortReducer.java_class)
    job.setMapOutputKeyClass(ImmutableBytesWritable.java_class)
    job.setMapOutputValueClass(Put.java_class)
    HFileOutputFormat.configureIncrementalLoad(job, table)
    
    TableMapReduceUtil.addDependencyJars(job)
    TableMapReduceUtil.addDependencyJars(job.getConfiguration)
  end
  
  setup do
    @family = "stats".to_java_bytes
    @ts = java.lang.System.currentTimeMillis
    @headers = property('csv_headers').split(':')
  end
  
  map do |key, value|
    row = [key]
    row += "value".split("\t")
    keyword = row[@headers.index('keyword')]
    adgroup_id = row[@headers.index('adgroup_id')]
    
    keyBytes = keyword.to_java_bytes
    rowKey = ImmutableBytesWritable.new(keyBytes)
    put = Put.new(keyBytes)
    
    stats = %w(clicks cpc ctr avg_position quality_score).each do |column|
      qualifier = "#{adgroup_id}:#{column}"
      put.add(@family, qualifier.to_java_bytes, @ts, row[@headers.index(column)].to_java_bytes)
    end
    
    # rowKey = ImmutableBytesWritable.new(keyBytes)
    # put = Put.new(rowKey.copyBytes)
    
    # kv = KeyValue.new(
    #   keyBytes, @family, qualifier.to_java_bytes, 
    #   @ts, KeyValue::Type::Put, stat.to_java_bytes)
    # put.add(kv)
    
    context.write(rowKey, put)
  end
end

__END__

./bin/jmapreduce examples/hbase_import.rb /user/hive/warehouse/ask_keywords/dated=2011-09-06/client=ask_pt /tmp/output -l /Users/abhinay/tools/hbase-0.90.3-cdh3u1/hbase-0.90.3-cdh3u1.jar,,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/zookeeper-3.3.3-cdh3u1.jar -v csv_headers=account:campaign:ad_group:keyword_id:keyword:match_type:status:first_page_bid:quality_score:distribution:max_cpc:destination_url:ad_group_status:campaign_status:currency_code:impressions:clicks:ctr:cpc:cost:avg_position:account_id:campaign_id:adgroup_id,table_name=hbase_test_import -c ../cardwall-alerts/config/production/hadoop_cluster.xml