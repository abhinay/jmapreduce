require 'java'

java_package 'org.fingertap.jmapreduce'

import java.io.IOException

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class JMapper < Mapper
  
  java_signature 'void setup(org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def setup(context)
    @key = Text.new
    @value = Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
    
    require script
    @job = JMapReduce.jobs[job_index]
    @job.set_conf(conf)
    @job.set_context(context)
    @job.set_properties(conf.get('jmapreduce.property'))
    
    @job.get_setup.call if @job.setup_exists
  end
  
  java_signature 'void map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def map(key, value, context)
    value = value.to_s
    
    if value.include?("\t")
      tokens = value.split("\t")
      key = tokens.first
      value = tokens[1..-1].join("\t")
    end
    
    if @job.mapper.nil?
      @key.set(key.to_s)
      @value.set(value.to_s)
      context.write(@key, @value)
      return
    end
    
    @job.mapper.call(key, @job.unpack(value))
  end
end