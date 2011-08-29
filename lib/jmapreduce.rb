require 'java'

import 'JMapReduceJob'
import 'MapperWrapper'
import 'ReducerWrapper'

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
  
  java_signature 'void main(String[])'
  def self.main(args)
    conf = org.apache.hadoop.conf.Configuration.new
    otherArgs = org.apache.hadoop.util.GenericOptionsParser.new(conf, args).getRemainingArgs
    if (otherArgs.size != 3)
      java.lang.System.err.println("Usage: JMapReduce <script> <in> <out>")
      java.lang.System.exit(2)
    end
    
    script = otherArgs[0]
    script_input = otherArgs[1]
    script_output = otherArgs[2]
    conf.set('jmapreduce.script.name', script)
    
    require script
    
    input = script_input
    output = script_output
    
    @@jobs.each_with_index do |job,index|
      conf.set('jmapreduce.job.index', index.to_s)
      
      if @@jobs.size > 1
        if index == @@jobs.size-1
          output = script_output
        else
          output = "#{script_output}-part-#{index}"
        end
      end

      job = org.apache.hadoop.mapreduce.Job.new(conf, job.name)
      job.setJarByClass(JMapReduce.to_java.getReifiedClass)
      job.setMapperClass(MapperWrapper.to_java.getReifiedClass)
      job.setReducerClass(ReducerWrapper.to_java.getReifiedClass)
      job.setOutputKeyClass(org.apache.hadoop.io.Text.to_java.getReifiedClass)
      job.setOutputValueClass(org.apache.hadoop.io.Text.to_java.getReifiedClass)

      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, org.apache.hadoop.fs.Path.new(input))
      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, org.apache.hadoop.fs.Path.new(output))
      job.waitForCompletion(true)
      
      input = output
    end
  end
end