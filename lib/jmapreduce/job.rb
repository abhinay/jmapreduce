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
    @key = Text.new
    @value = Text.new
  end
  
  def setup(&blk)
    @setup = blk
  end
  
  def map(&blk)
    @mapper = blk
  end
  
  def reduce(&blk)
    @reducer = blk
  end
  
  def mapper
    @mapper
  end
  
  def reducer
    @reducer
  end
  
  def get_setup
    @setup
  end
  
  def setup_exists
    !@setup.nil?
  end
  
  def set_name(name)
    @name = name
  end
  
  def name
    @name
  end
  
  def map_tasks(num_of_tasks)
    @map_tasks = num_of_tasks
  end
  
  def reduce_tasks(num_of_tasks)
    @reduce_tasks = num_of_tasks
  end
  
  def num_of_reduce_tasks
    return @reduce_tasks if @reduce_tasks
    @reducer ? 1 : 0
  end
  
  def set_mapreduce(blk)
    self.instance_eval(&blk)
  end
  
  def context
    @context
  end
  
  def set_context(context)
    @context = context
  end
  
  def set_conf(conf)
    @conf = conf
  end
  
  def conf
    @conf
  end
  
  def custom_job(&blk)
    @custom_job = blk
  end
  
  def before_job(&blk)
    @before_job = blk
  end
  
  def get_custom_job
    @custom_job
  end
  
  def before_job_hook
    @before_job
  end
  
  def emit(key, value)
    @key.set(key.to_s)
    @value.set(pack(value))
    @context.write(@key, @value)
  end
  
  def set_properties(properties)
    return unless properties
    
    @properties = {}
    props = properties.split(',')
    props.each do |property|
      key,value = *property.split('=')
      if key == 'json'
        JsonProperty.parse(value).each do |(k,v)|
          @properties[k] = v
        end
      else
        @properties[key] = value
      end
    end
  end
  
  def property(key)
    @properties[key] if @properties
  end
  
  def pack(value)
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