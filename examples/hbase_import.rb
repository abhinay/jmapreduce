import org.fingertap.jmapreduce.JMapReduce

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.PutSortReducer
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil

JMapReduce.job "Keywords bulk import" do
  reduce_tasks 0
  
  custom_job do |conf|
    hbase_conf = HBaseConfiguration.create(conf)
    hbase_conf.set('hbase.zookeeper.quorum', 'hbase-master.hadoop.forward.co.uk')
    job = Job.new(hbase_conf, "#{property('client')} keywords bulk import")

    TableMapReduceUtil.initTableReducerJob(property('table_name'), nil, job)
    TableMapReduceUtil.addDependencyJars(job)
    TableMapReduceUtil.addDependencyJars(job.getConfiguration)
    
    job.setMapOutputValueClass(Put.java_class)
    job
  end
  
  setup do
    @family = "stats".to_java_bytes
    @ts = java.lang.System.currentTimeMillis
    @headers = property('csv_headers').split(':')
  end
  
  map do |key, value|
    row = [key] + value.split("\t")
    keyword = row[@headers.index('keyword')]
    adgroup_id = row[@headers.index('adgroup_id')]
    
    keyBytes = keyword.to_java_bytes
    rowKey = ImmutableBytesWritable.new(keyBytes)
    put = Put.new(keyBytes)
    
    stats = %w(clicks cpc ctr avg_position quality_score).each do |column|
      qualifier = "#{adgroup_id}:#{column}"
      put.add(@family, qualifier.to_java_bytes, @ts, row[@headers.index(column)].to_java_bytes)
    end
    
    context.write(rowKey, put)
  end
end

__END__

mandy-rm /tmp/output -c ../cardwall-alerts/config/production/hadoop_cluster.xml && ./bin/jmapreduce examples/hbase_import.rb /user/hive/warehouse/ask_keywords/dated=2011-09-06/client=ask_pt /tmp/output -l /Users/abhinay/tools/hbase-0.90.3-cdh3u1/hbase-0.90.3-cdh3u1.jar,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/zookeeper-3.3.3-cdh3u1.jar,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/guava-r06.jar -v csv_headers=account:campaign:ad_group:keyword_id:keyword:match_type:status:first_page_bid:quality_score:distribution:max_cpc:destination_url:ad_group_status:campaign_status:currency_code:impressions:clicks:ctr:cpc:cost:avg_position:account_id:campaign_id:adgroup_id,table_name=abs_test_keywords -c ../cardwall-alerts/config/production/hadoop_cluster.xml

rm -rf /tmp/output* && ./bin/jmapreduce examples/hbase_import.rb /Users/abhinay/Desktop/data.tsv /tmp/output -l /Users/abhinay/tools/hbase-0.90.3-cdh3u1/hbase-0.90.3-cdh3u1.jar,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/zookeeper-3.3.3-cdh3u1.jar,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/guava-r06.jar -v csv_headers=account:campaign:ad_group:keyword_id:keyword:match_type:status:first_page_bid:quality_score:distribution:max_cpc:destination_url:ad_group_status:campaign_status:currency_code:impressions:clicks:ctr:cpc:cost:avg_position:account_id:campaign_id:adgroup_id,table_name=abs_test_keywords