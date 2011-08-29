require 'java'

import java.io.IOException

class JReducer < org.apache.hadoop.mapreduce.Reducer
  java_signature 'void setup(org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def setup(context)
    @key = org.apache.hadoop.io.Text.new
    @value = org.apache.hadoop.io.Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
  
    require script
    @reducer = JMapReduce.jobs[job_index].reducer
  end
  
  java_signature 'void reduce(org.apache.hadoop.io.Text, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def reduce(key, values, context)
    if @reducer.nil?
      values.each do |value|
        context.write(key, value)
      end
      return
    end
    
    results = []
    @reducer.call(key, values.map{|v|v.to_s}, results)
    results.each do |tuple|
      tuple.each do |(k,v)|
        @key.set(k.to_s)
        @value.set(v.to_s)
        context.write(@key, @value)
      end
    end
  end
end