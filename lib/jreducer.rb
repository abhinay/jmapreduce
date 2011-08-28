require 'java'

import java.io.IOException

class JReducer < org.apache.hadoop.mapreduce.Reducer

  @key = org.apache.hadoop.io.Text.new
  @value = org.apache.hadoop.io.Text.new

  java_signature 'void setup(org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def setup(context)
    conf = context.getConfiguration
    script = conf.set('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
  
    require script
    @reducer = JMapReduce.jobs.get(job_index).reducer
  end

  java_signature 'void reduce(org.apache.hadoop.io.Text, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def reduce(key, values, context)
    tuples = @reducer.call(key,values)
    tuples.each do |(k,v)|
      @key.set(k)
      @value.set(v)
      context.write(@key, @value)
    end
  end
end