require 'java'

java_package 'org.fingertap.jmapreduce'

import org.fingertap.jmapreduce.ValuePacker
import org.fingertap.jmapreduce.JsonProperty

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable

class JMapReduceJob
  def initialize
    @jmapreduce_job_key = Text.new
    @jmapreduce_job_value = Text.new
  end
  
  def setup(&blk)
    @jmapreduce_job_setup = blk
  end
  
  def map(&blk)
    @jmapreduce_job_mapper = blk
  end
  
  def reduce(&blk)
    @jmapreduce_job_reducer = blk
  end
  
  def mapper
    @jmapreduce_job_mapper
  end
  
  def reducer
    @jmapreduce_job_reducer
  end
  
  def set_last_reducer
    @jmapreduce_job_last_reducer = true
  end
  
  def set_last_mapper
    @jmapreduce_job_last_mapper = true
  end
  
  def is_last_reducer
    @jmapreduce_job_last_reducer
  end
  
  def is_last_mapper
    @jmapreduce_job_last_mapper
  end
  
  def running_last_emit
    @jmapreduce_job_running_last_emit = true
  end
  
  def get_setup
    @jmapreduce_job_setup
  end
  
  def setup_exists
    !@jmapreduce_job_setup.nil?
  end
  
  def set_name(name)
    @jmapreduce_job_name = name
  end
  
  def name
    @jmapreduce_job_name
  end
  
  def map_tasks(num_of_tasks)
    @jmapreduce_job_map_tasks = num_of_tasks
  end
  
  def reduce_tasks(num_of_tasks)
    @jmapreduce_job_reduce_tasks = num_of_tasks
  end
  
  def num_of_reduce_tasks
    return @jmapreduce_job_reduce_tasks if @jmapreduce_job_reduce_tasks
    @jmapreduce_job_reducer ? 1 : 0
  end
  
  def set_mapreduce(blk)
    self.instance_eval(&blk)
  end
  
  def context
    @jmapreduce_job_context
  end
  
  def set_context(context)
    @jmapreduce_job_context = context
  end
  
  def set_conf(conf)
    @jmapreduce_job_conf = conf
  end
  
  def conf
    @jmapreduce_job_conf
  end
  
  def custom_job(&blk)
    @jmapreduce_job_custom_job = blk
  end
  
  def before_job(&blk)
    @jmapreduce_job_before_job = blk
  end
  
  def get_custom_job
    @jmapreduce_job_custom_job
  end
  
  def before_job_hook
    @jmapreduce_job_before_job
  end
  
  def emit(key, value)
    @jmapreduce_job_key.set(key.to_s)
    @jmapreduce_job_value.set(pack(value))
    @jmapreduce_job_context.write(@jmapreduce_job_key, @jmapreduce_job_value)
  end
  
  def set_properties(properties)
    return unless properties
    
    @jmapreduce_job_properties = {}
    props = properties.split(',')
    props.each do |property|
      key,value = *property.split('=')
      if key == 'json'
        JsonProperty.parse(value).each do |(k,v)|
          @jmapreduce_job_properties[k] = v
        end
      else
        @jmapreduce_job_properties[key] = value
      end
    end
  end
  
  def property(key)
    @jmapreduce_job_properties[key.to_s] if @jmapreduce_job_properties
  end
  
  def pack(value)
    return value.to_s if @jmapreduce_job_running_last_emit
    
    case value
    when Integer, Float, String, Array then return ValuePacker.pack(value)
    when Symbol then return ValuePacker.pack(value.to_s)
    when Hash then 
      h = value.inject({}) do |h, (k,v)|
        k = k.to_s if k.is_a?(Symbol)
        h[k] = v
        h
      end
      return ValuePacker.pack(h)
    else raise "Unknown value type #{value.class}. Only following types allowed: Integer, Float, String, Symbol, Array and Hash"
    end
  end
  
  def unpack(value)
    obj = ValuePacker.unpack(value.to_s)
    case obj
    when java.util.HashMap then
      return obj.inject({}) do |h, (k,v)|
        h[ruby_value(k)] = ruby_value(v)
        h
      end
    when java.util.ArrayList then
      return obj.map { |v| ruby_value(v) }
    else return obj
    end
  end
  
  def ruby_value(v)
    return v.intValue if v.isIntegerType
    return v.floatValue if v.isFloatType
    return v.asString if v.isRawType
    return v.asBoolean if v.isBooleanType
    return nil if v.isNil
    
    return v.asMap.inject({}) do |h, (k,v)|
      h[ruby_value(k)] = ruby_value(v)
      h
    end if v.isMapType
    
    return v.asArray.map do |v| 
      ruby_value(v) 
    end if v.isArrayType
    
    return v
  end
end