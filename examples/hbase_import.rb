import org.fingertap.jmapreduce.JMapReduce

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.PutSortReducer
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil

JMapReduce.job "Calculating adgroup stats for keywords for #{JMapReduce.property('client')}" do
  map_tasks 20
  reduce_tasks 5
  
  setup do
    HEADERS = property('csv_headers').split(":")
  end
  
  map do |key, value|
    row = [key] + value.split("\t")
    keyword = row[HEADERS.index('keyword')]
    adgroup_id = row[HEADERS.index('adgroup_id')]
    
    stats = %w(clicks cpc ctr avg_position quality_score).inject({}) do |h,column|
      h[column] = row[HEADERS.index(column)]
      h
    end
    
    emit(keyword, { adgroup_id => stats })
  end
  
  reduce do |keyword, values|
    keyword_stats = {}
    stat_rows = {}
    max_clicks = -1
    
    values.each do |adgroup_stats|
      adgroup_stats.each do |(adgroup_id,stats)|
        keyword_stats = stats if stats['clicks'].to_i > max_clicks
        stats.keys.each do |key|
          stat_rows["#{adgroup_id}:#{key}"] = stats[key]
        end
      end
    end
    
    emit(keyword, keyword_stats.merge(stat_rows))
  end
end

JMapReduce.job "Keywords bulk import for #{JMapReduce.property('client')}" do
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
  
  map do |keyword, stats|
    keyBytes = keyword.to_java_bytes
    rowKey = ImmutableBytesWritable.new(keyBytes)
    put = Put.new(keyBytes)
    
    stats.each do |(qualifier,stat)|
      put.add(@family, qualifier.to_java_bytes, @ts, stat.to_s.to_java_bytes)
    end
    
    context.write(rowKey, put)
  end
end

__END__

mandy-rm /tmp/output -c ../cardwall-alerts/config/production/hadoop_cluster.xml && ./bin/jmapreduce examples/hbase_import.rb /user/hive/warehouse/ask_keywords/dated=2011-09-06/client=ask_pt /tmp/output -l /Users/abhinay/tools/hbase-0.90.3-cdh3u1/hbase-0.90.3-cdh3u1.jar,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/zookeeper-3.3.3-cdh3u1.jar,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/guava-r06.jar -v csv_headers=account:campaign:ad_group:keyword_id:keyword:match_type:status:first_page_bid:quality_score:distribution:max_cpc:destination_url:ad_group_status:campaign_status:currency_code:impressions:clicks:ctr:cpc:cost:avg_position:account_id:campaign_id:adgroup_id,table_name=abs_test_keywords,client=ask_pt -c ../cardwall-alerts/config/production/hadoop_cluster.xml

rm -rf /tmp/output* && ./bin/jmapreduce examples/hbase_import.rb /Users/abhinay/Desktop/data.tsv /tmp/output -l /Users/abhinay/tools/hbase-0.90.3-cdh3u1/hbase-0.90.3-cdh3u1.jar,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/zookeeper-3.3.3-cdh3u1.jar,/Users/abhinay/tools/hbase-0.90.3-cdh3u1/lib/guava-r06.jar -v csv_headers=account:campaign:ad_group:keyword_id:keyword:match_type:status:first_page_bid:quality_score:distribution:max_cpc:destination_url:ad_group_status:campaign_status:currency_code:impressions:clicks:ctr:cpc:cost:avg_position:account_id:campaign_id:adgroup_id,table_name=abs_test_keywords,client=ask_pt