require 'java'

java_package 'org.fingertap.jmapreduce'

import java.io.IOException

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import org.fingertap.jmapreduce.JMapReduce

class JMapper < Mapper
  
  java_signature 'void setup(org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def setup(context)
    @jmapreduce_mapper_key = Text.new
    @jmapreduce_mapper_value = Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
    JMapReduce.set_properties(conf.get('jmapreduce.property'))
    
    require script
    @jmapreduce_mapper_job = JMapReduce.jobs[job_index]
    @jmapreduce_mapper_job.set_conf(conf)
    @jmapreduce_mapper_job.set_context(context)
    @jmapreduce_mapper_job.set_properties(conf.get('jmapreduce.property'))
    @jmapreduce_mapper_job.running_last_emit if conf.get('jmapreduce.last_job.mapper')
    
    @jmapreduce_mapper_job.get_setup.call if @jmapreduce_mapper_job.setup_exists
  end
  
  java_signature 'void map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def map(key, value, context)
    value = value.to_s
    
    if value.include?("\t")
      tokens = value.split("\t")
      key = tokens.first
      value = tokens[1..-1].join("\t")
    end
    
    if @jmapreduce_mapper_job.mapper.nil?
      @jmapreduce_mapper_key.set(key.to_s)
      @jmapreduce_mapper_value.set(value.to_s)
      context.write(@jmapreduce_mapper_key, @jmapreduce_mapper_value)
      return
    end
    
    @jmapreduce_mapper_job.mapper.call(key, @jmapreduce_mapper_job.unpack(value))
  end
end