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

<script src="https://bitbucket.org/abhinaymehta/jmapreduce/src/a53594ca887c/examples/wordcount.rb?embed=t"></script>

Author
-------

Abhinay Mehta <abhinay.mehta@gmail.com>

Copyright
---------

License: Apache License