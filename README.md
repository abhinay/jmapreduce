JMapReduce
==========

JMapReduce provides a simple [mandy](http://github.com/forward/mandy "Mandy") like DSL to run map/reduce jobs on Hadoop in the JVM via JRuby. Because it runs in the JVM, you have access to all the Java objects provided to the Map/Reduce jobs at runtime and can leverage other Java libraries inside your jobs.

Install
-------

> gem install jmapreduce

Usage
-----

1. Install Hadoop and set HADOOP_HOME env variable
2. To run a jmapreduce script:
> jmapreduce examples/wordcount.rb examples/alice.txt /tmp/jmapreduce-output

3. For full list of options, including how to run your scripts against a Hadoop cluster run:
> jmapreduce -h

Some Rules
----------

* Mappers and reducers can emit Integers, Floats, Strings, Arrays and Hashes
* Arrays and Hashes can only be built up of Integers, Floats, Strings, Arrays and Hashes
* You can chain map/reduce jobs like the example below. The output of one map/reduce job will be the input of the next job
* Be sure the very last thing you emit in your last job are Strings otherwise you will see binary data in your eventual output

Example
-------
    
    import org.fingertap.jmapreduce.JMapReduce
    
    JMapReduce.job 'Count' do
      reduce_tasks 1
        
      map do |key, value|
        value.split.each do |word|
            emit(word, 1)
        end
      end
    
      reduce do |word, counts|
        sum = 0
        counts.each {|count| sum += count }
        emit(word, sum)
      end
    end
    
    JMapReduce.job "Histogram" do
      setup do
        RANGES = [0..10, 11..20, 21..50, 51..100, 101..200, 201..300, 301..10_000, 10_001..99_999]
      end
        
      map do |word, sum|
        range = RANGES.find {|range| range.include?(sum) }
        emit("#{range.first.to_s}-#{range.last.to_s}", 1)
      end
        
      reduce do |range, counts|
        total = counts.inject(0) {|sum,count| sum+count }
        emit(range, '|'*(total/20))
      end
    end
    
Using Java classes Example 
--------------------------
    
    import org.fingertap.jmapreduce.JMapReduce
    
    import java.util.StringTokenizer
    
    JMapReduce.job 'Count' do
      reduce_tasks 1
        
      map do |key, value|
        tokenizer = StringTokenizer.new(value, " ")
        while(tokenizer.hasMoreTokens)
          word = tokenizer.nextToken
          emit(word, 1)
        end
      end
    
      reduce do |word, counts|
        sum = 0
        counts.each {|count| sum += count }
        emit(word, sum.to_s)
      end
    end
    
Running a custom org.apache.hadoop.mapreduce.Job Example
--------------------------------------------------------

The example below shows how you can provide a custom job to run and have direct access to the context in your map or reduce blocks so you can write out objects of the class you specified in your custom job.
    
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
        someColumnValue = row[1].to_java_bytes
        someOtherColumnValue = row[2].to_java_bytes
        
        put = Put.new(row_key)
        put.add(@family, "someColumn".to_java_bytes, @ts, someColumnValue)
        put.add(@family, "someOtherColumn".to_java_bytes, @ts, someOtherColumnValue)
        
        context.write(ImmutableBytesWritable.new(row_key), put)
      end
    end
    
To run the above example, run:
> jmapreduce examples/hbase_import.rb /path/to/tsv/file /output/path -l $HBASE_HOME/hbase.jar,$HBASE_HOME/lib/zookeeper.jar,$HBASE_HOME/lib/guava.jar -v table_name=someTableName

Example Hadoop Conf XML File
----------------------------

    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
      <property>
        <name>fs.default.name</name>
        <value>hdfs://name-node.address:fs-port/</value>
      </property>
      <property>
        <name>mapred.job.tracker</name>
        <value>job.tracker.address:job-tracker-port</value>
      </property>
    </configuration>

Author
-------

Abhinay Mehta <abhinay.mehta@gmail.com>

Copyright
---------

License: Apache License