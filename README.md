JMapReduce
==========

Description
-----------

JMapReduce is JRuby Map/Reduce Framework built on top of the Hadoop Distributed computing platform.
Inspired by [mandy](http://github.com/forward/mandy "Mandy") but runs the map/reduce jobs on the JVM.

Install
-------

> gem install jmapreduce

Usage
-----

1. Run Hadoop cluster on your machines and set HADOOP_HOME env variable.
2. put files into your hdfs. eg) test/inputs/file1
3. Now you can run 'jmapreduce' like below:
> $ jmapreduce examples/wordcount.rb test/inputs/file1 test/outputs
(Job results will be saved in your hdfs test/outputs/part-*)
4. You can also chain map/reduce jobs like the example below. The output of one map/reduce job will be the input of the next job
5. For full list of options, run:
> $ jmapreduce -h

Example 
-------
see also examples/wordcount.rb

> import org.fingertap.jmapreduce.JMapReduce
> 
> JMapReduce.job 'Count' do
>   reduce_tasks 1
>   
>   map do |key, value|
>     value.split.each do |word|
>       emit(word, 1)
>     end
>   end
>   
>   reduce do |key, values|
>     sum = 0
>     values.each {|v| sum += v.to_i }
>     emit(key, sum)
>   end
> end
> 
> JMapReduce.job "Histogram" do
>   setup do
>     RANGES = [0..10, 11..20, 21..50, 51..100, 101..200, 201..300, 301..10_000, 10_001..99_999]
>   end
>   
>   map do |word, count|
>     range = RANGES.find {|range| range.include?(count.to_i) }
>     emit("#{range.first.to_s.rjust(5,'0')}-#{range.last.to_s.rjust(5,'0')}", 1)
>   end
> 
>   reduce do |range, counts|
>     total = counts.inject(0) {|sum,count| sum+count.to_i }
>     emit(range, '|'*(total/20))
>   end
> end

Author
-------

Abhinay Mehta <abhinay.mehta@gmail.com>

Copyright
---------

License: Apache License