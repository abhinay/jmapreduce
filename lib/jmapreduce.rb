require 'java'

import 'JMapper'
import 'JReducer'

class JMapReduce
  def map(blk)
    @mapper = blk
  end
  
  def reduce(blk)
    @reducer = blk
  end
  
  def mapper
    @mapper
  end
  
  def reducer
    @reducer
  end
  
  def set_name(name)
    @name = name
  end
  
  def name
    @name
  end
  
  def self.jobs
    @@jobs
  end
  
  def self.job(name, blk)
    job = JMapReduce.new
    job.set_name(name)
    @@jobs ||= []
    @@jobs << job
    blk.call(job)
  end
  
  java_signature 'void main(String[])'
  def self.main(args)
    require 'run'
    @@jobs.each_with_index do |job,index|
      conf = org.apache.hadoop.conf.Configuration.new
      conf.set('jmapreduce.job.index', index.to_s)
      otherArgs = org.apache.hadoop.util.GenericOptionsParser.new(conf, args).getRemainingArgs
      
      if (otherArgs.size != 2)
        java.lang.System.err.println("Usage: JMapReduce <in> <out>")
        java.lang.System.exit(2)
      end
      
      job = org.apache.hadoop.mapreduce.Job.new(conf, job.name)
      job.setJarByClass(JMapReduce.to_java.getReifiedClass)
      job.setMapperClass(JMapper.to_java.getReifiedClass)
      job.setReducerClass(JReducer.to_java.getReifiedClass)
      job.setOutputKeyClass(org.apache.hadoop.io.Text.to_java.getReifiedClass)
      job.setOutputValueClass(org.apache.hadoop.io.Text.to_java.getReifiedClass)
      
      FileInputFormat.addInputPath(job, org.apache.hadoop.fs.Path.new(otherArgs[0]))
      FileOutputFormat.setOutputPath(job, org.apache.hadoop.fs.Path.new(otherArgs[1]))
      java.lang.System.exit(job.waitForCompletion(true) ? 0 : 1)
    end
  end
end