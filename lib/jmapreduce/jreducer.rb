require 'java'

java_package 'org.fingertap.jmapreduce'

import java.io.IOException

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import org.fingertap.jmapreduce.JMapReduce

class JReducer < Reducer
  
  java_signature 'void setup(org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def setup(context)
    @jmapreduce_reducer_key = Text.new
    @jmapreduce_reducer_value = Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
    JMapReduce.set_properties(conf.get('jmapreduce.property'))
    
    require script
    @jmapreduce_reducer_job = JMapReduce.jobs[job_index]
    @jmapreduce_reducer_job.set_conf(conf)
    @jmapreduce_reducer_job.set_context(context)
    @jmapreduce_reducer_job.set_properties(conf.get('jmapreduce.property'))
    
    @jmapreduce_reducer_job.get_setup.call if @jmapreduce_reducer_job.setup_exists
  end
  
  java_signature 'void reduce(org.apache.hadoop.io.Text, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def reduce(key, values, context)
    if @jmapreduce_reducer_job.reducer.nil?
      values.each do |value|
        context.write(key, value)
      end
      return
    end
    
    @jmapreduce_reducer_job.reducer.call(key, values.map{ |v| @jmapreduce_reducer_job.unpack(v) })
  end
end