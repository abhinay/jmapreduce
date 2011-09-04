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
  
  def set_context(context)
    @context = context
  end
  
  def set_conf(conf)
    @conf = conf
  end
  
  def conf
    @conf
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
    when Hash then return ValuePacker.pack(value)
    when Array then return ValuePacker.pack(value)
    else return ValuePacker.pack(value.to_java)
    end
  end
  
  def unpack(value)
    obj = ValuePacker.unpack(value.to_s)
    case obj
    when java.util.ArrayList then
      return obj.map { |v|
        if v.isIntegerType
          v.intValue
        elsif v.isFloatType
          v.floatValue
        elsif v.isRawType
          v.asString
        elsif v.isBooleanType
          v.asBoolean
        else
          v
        end
      }
    else return obj
    end
  end
end