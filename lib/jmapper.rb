require 'java'

import java.io.IOException

class JMapper < org.apache.hadoop.mapreduce.Mapper
  @key = org.apache.hadoop.io.Text.new
  @value = org.apache.hadoop.io.Text.new

  java_signature 'void setup(org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def setup(context)
    conf = context.getConfiguration
    script = conf.set('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
  
    require script
    @mapper = JMapReduce.jobs.get(job_index).mapper
  end

  java_signature 'void map(java.lang.Object, org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def map(key, value, context)
    tuples = @mapper.call(value.to_s)
    tuples.each do |(k,v)|
      @key.set(k)
      @value.set(v)
      context.write(@key, @value)
    end
  end
end