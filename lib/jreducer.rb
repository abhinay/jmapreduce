require 'java'

import java.io.IOException

class JReducer < org.apache.hadoop.mapreduce.Reducer
  
  @key = org.apache.hadoop.io.Text.new
  @value = org.apache.hadoop.io.Text.new
  
  def setup(context)
    require 'run'
    conf = context.getConfiguration
    job_index = conf.get('jmapreduce.job.index').to_i
    @reducer = JMapReduce.jobs.get(job_index).reducer
  end
  
  java_signature 'void map(org.apache.hadoop.io.Text, java.lang.Iterable, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def reduce(key, values, context)
    tuples = @reducer.call(key,values)
    tuples.each do |(k,v)|
      @key.set(k)
      @value.set(v)
      context.write(@key, @value)
    end
  end
end