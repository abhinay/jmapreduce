require 'java'

java_package 'org.fingertap.jmapreduce'

import java.io.IOException

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class JReducer < Reducer
  java_signature 'void setup(org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def setup(context)
    @key = Text.new
    @value = Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
    
    require script
    job = JMapReduce.jobs[job_index]
    job.set_context(context, @key, @value)
    job.set_conf(conf)
    job.get_setup.call if job.setup_exists
    job.set_properties(conf.get('jmapreduce.property'))
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
    
    @reducer.call(key, values.map{|v|v.to_s})
  end
end