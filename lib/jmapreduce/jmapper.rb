require 'java'

import java.io.IOException

class JMapper < org.apache.hadoop.mapreduce.Mapper
  java_signature 'void setup(org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def setup(context)
    @key = org.apache.hadoop.io.Text.new
    @value = org.apache.hadoop.io.Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
    
    require script
    job = JMapReduce.jobs[job_index]
    job.set_context(context, @key, @value)
    job.get_setup.call if job.setup_exists
    job.set_properties(conf.get('jmapreduce.property'))
    @mapper = job.mapper
  end
  
  java_signature 'void map(java.lang.Object, org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def map(key, value, context)
    value = value.to_s
    key,value = *value.split("\t") if value.include?("\t")
    
    if @mapper.nil?
      @key.set(key.to_s)
      @value.set(value.to_s)
      context.write(@key, @value)
      return
    end
    
    @mapper.call(key, value)
  end
end