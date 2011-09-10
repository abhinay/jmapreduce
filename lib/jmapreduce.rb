require 'java'

java_package 'org.fingertap.jmapreduce'

import org.fingertap.jmapreduce.JsonProperty
import org.fingertap.jmapreduce.JMapReduceJob
import org.fingertap.jmapreduce.MapperWrapper
import org.fingertap.jmapreduce.ReducerWrapper

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class JMapReduce
  def self.jobs
    @@jobs
  end
  
  def self.job(name, blk)
    job = JMapReduceJob.new
    job.set_name(name)
    @@jobs ||= []
    @@jobs << job
    job.set_mapreduce(blk)
  end
  
  def self.set_properties(properties)
    return unless properties
    
    @@properties = {}
    props = properties.split(',')
    props.each do |property|
      key,value = *property.split('=')
      if key == 'json'
        JsonProperty.parse(value).each do |(k,v)|
          @@properties[k] = v
        end
      else
        @@properties[key] = value
      end
    end
  end
  
  def self.property(key)
    @@properties[key] if @@properties
  end
  
  java_signature 'void main(String[])'
  def self.main(args)
    conf = Configuration.new
    otherArgs = GenericOptionsParser.new(conf, args).getRemainingArgs
    
    if (otherArgs.size < 3)
      java.lang.System.err.println("Usage: JMapReduce <script> <in> <out>")
      java.lang.System.exit(2)
    end
    
    script = otherArgs[0]
    script_input = otherArgs[1]
    script_output = otherArgs[2]
    conf.set('jmapreduce.script.name', script)
    
    if otherArgs.size > 3
      (3..(otherArgs.size-1)).each do |index|
        if otherArgs[index].include?('=')
          conf.set('jmapreduce.property', otherArgs[index])
          set_properties(otherArgs[index])
        end
      end
    end
    
    require script
    input = script_input
    output = script_output
    tmp_outputs = []
    
    @@jobs.each_with_index do |jmapreduce_job,index|
      jmapreduce_job.set_conf(conf)
      jmapreduce_job.set_properties(conf.get('jmapreduce.property'))
      
      conf.set('jmapreduce.job.index', index.to_s)
      
      if @@jobs.size > 1
        if index == @@jobs.size-1
          output = script_output
        else
          output = "#{script_output}-part-#{index}"
          tmp_outputs << output
        end
      end
      
      if jmapreduce_job.get_custom_job
        job = jmapreduce_job.get_custom_job.call(conf)
      else
        job = Job.new(conf, jmapreduce_job.name)
        job.setOutputKeyClass(Text.java_class)
        job.setOutputValueClass(Text.java_class)
      end
      
      job.setJarByClass(JMapReduce.java_class)
      job.setMapperClass(MapperWrapper.java_class)
      job.setReducerClass(ReducerWrapper.java_class)
      job.setNumReduceTasks(jmapreduce_job.num_of_reduce_tasks)
      
      FileInputFormat.addInputPath(job, Path.new(input))
      FileOutputFormat.setOutputPath(job, Path.new(output))
      
      job.waitForCompletion(true)
      input = output
    end
    
    # clean up
    tmp_outputs.each do |tmp_output|
      tmp_path = Path.new(tmp_output)
      hdfs = tmp_path.getFileSystem(conf)
      hdfs.delete(tmp_path, true) if hdfs.exists(tmp_path)
    end
  end
end