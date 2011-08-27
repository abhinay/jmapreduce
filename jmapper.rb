require 'java'

import java.io.IOException

class JMapper < org.apache.hadoop.mapreduce.Mapper
  include Job
  
  @key = org.apache.hadoop.io.Text.new
  @value = org.apache.hadoop.io.Text.new
  
  java_signature 'void setup(org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def setup(context)
    require 'run'
    @mapper = JMapReduce.jobs.first.mapper
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